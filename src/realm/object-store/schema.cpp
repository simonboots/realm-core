////////////////////////////////////////////////////////////////////////////
//
// Copyright 2015 Realm Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////

#include <realm/object-store/schema.hpp>

#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/object_store.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>

#include <algorithm>
#include <queue>
#include <unordered_set>

using namespace realm;

namespace realm {
bool operator==(Schema const& a, Schema const& b) noexcept
{
    return static_cast<Schema::base const&>(a) == static_cast<Schema::base const&>(b);
}
} // namespace realm

Schema::Schema() noexcept = default;
Schema::~Schema() = default;
Schema::Schema(Schema const&) = default;
Schema::Schema(Schema&&) noexcept = default;
Schema& Schema::operator=(Schema const&) = default;
Schema& Schema::operator=(Schema&&) noexcept = default;

Schema::Schema(std::initializer_list<ObjectSchema> types)
    : Schema(base(types))
{
}

Schema::Schema(base types) noexcept
    : base(std::move(types))
{
    std::sort(begin(), end(), [](ObjectSchema const& lft, ObjectSchema const& rgt) {
        return lft.name < rgt.name;
    });
}

Schema::iterator Schema::find(StringData name) noexcept
{
    auto it = std::lower_bound(begin(), end(), name, [](ObjectSchema const& lft, StringData rgt) {
        return lft.name < rgt;
    });
    if (it != end() && it->name != name) {
        it = end();
    }
    return it;
}

Schema::const_iterator Schema::find(StringData name) const noexcept
{
    return const_cast<Schema*>(this)->find(name);
}

Schema::iterator Schema::find(ObjectSchema const& object) noexcept
{
    return find(object.name);
}

Schema::const_iterator Schema::find(ObjectSchema const& object) const noexcept
{
    return const_cast<Schema*>(this)->find(object);
}

Schema::iterator Schema::find(TableKey table_key) noexcept
{
    if (!table_key) {
        return end();
    }
    // FIXME: Faster lookup than linear search.
    return std::find_if(begin(), end(), [table_key](const ObjectSchema& os) {
        return os.table_key == table_key;
    });
}

Schema::const_iterator Schema::find(TableKey table_key) const noexcept
{
    return const_cast<Schema*>(this)->find(table_key);
}

namespace {

struct CheckObjectPath {
    const ObjectSchema& object;              // the schema to check
    std::string path;                        // a printable path for error messaging
    std::unordered_set<std::string> visited; // the list of object types encountered along this path so far
};

// a non-recursive search that returns a property path to the first embedded object cycle detected
std::string do_check(Schema const& schema, const ObjectSchema& start)
{
    std::queue<CheckObjectPath> to_visit;
    to_visit.push(CheckObjectPath{start, start.name, {start.name}});

    while (to_visit.size() > 0) {
        auto current = to_visit.front();
        for (auto& prop : current.object.persisted_properties) {
            if (prop.type == PropertyType::Object) {
                auto it = schema.find(prop.object_type);
                REALM_ASSERT(it != schema.end()); // this succeeds if the schema is otherwise valid
                if (!it->is_embedded) {
                    // the server does support links to top level objects (serialized as a PK)
                    // so if we encounter this type of link, no need to check further along this path
                    continue;
                }
                auto next_path = current.path + "." + prop.name;
                if (current.visited.find(prop.object_type) != current.visited.end()) {
                    return next_path;
                }
                auto visited_copy = current.visited;
                visited_copy.insert(current.object.name);
                to_visit.push(CheckObjectPath{*it, next_path, std::move(visited_copy)});
            }
        }
        to_visit.pop();
    }
    return "";
}

void check_for_embedded_objects_loop(Schema const& schema, std::vector<ObjectSchemaValidationException>& exceptions)
{
    // A prerequisite for an embedded object loop is that there are links orginating from an embedded object
    // so we only need to run this check from embedded objects. This is an optimization to exclude entire
    // object graphs which do not contain embedded objects.
    for (auto const& object : schema) {
        if (object.is_embedded) {
            std::string loop = do_check(schema, object);
            if (!loop.empty()) {
                exceptions.push_back(
                    util::format("Cycles containing embedded objects are not currently supported: '%1'", loop));
            }
        }
    }
}

} // end anonymous namespace

std::unordered_set<std::string> Schema::get_embedded_object_orphans() const
{
    std::queue<const ObjectSchema*> to_check;
    for (auto& object : *this) {
        if (!object.is_embedded) {
            to_check.push(&object);
        }
    }
    // Perform a breadth-first search of the schema graph to discover all object
    // types which are reachable from any of the root types
    std::unordered_set<const ObjectSchema*> reachable;
    while (!to_check.empty()) {
        auto object = to_check.front();
        reachable.insert(object);
        for (auto& prop : object->persisted_properties) {
            if (prop.type == PropertyType::Object) {
                auto it = find(prop.object_type);
                REALM_ASSERT(it != this->end());
                if (it->is_embedded && reachable.insert(&*it).second) {
                    to_check.push(&*it);
                }
            }
        }
        to_check.pop();
    }
    // Any object types which weren't found above are orphans
    std::unordered_set<std::string> orphans;
    for (auto& object : *this) {
        if (object.is_embedded && !reachable.count(&object)) {
            orphans.insert(object.name);
        }
    }
    return orphans;
}


void Schema::validate(uint64_t validation_mode) const
{
    std::vector<ObjectSchemaValidationException> exceptions;

    // As the types are added sorted by name, we can detect duplicates by just looking at the following element.
    auto find_next_duplicate = [&](const_iterator start) {
        return std::adjacent_find(start, cend(), [](ObjectSchema const& lft, ObjectSchema const& rgt) {
            return lft.name == rgt.name;
        });
    };

    for (auto it = find_next_duplicate(cbegin()); it != cend(); it = find_next_duplicate(++it)) {
        exceptions.push_back(
            ObjectSchemaValidationException("Type '%1' appears more than once in the schema.", it->name));
    }

    const bool for_sync = validation_mode & SchemaValidationMode::Sync;
    for (auto const& object : *this) {
        object.validate(*this, exceptions, for_sync);
    }

    // TODO: remove this client side check once the server supports it
    // or generates a better error message.
    if (exceptions.empty()) {
        // only attempt to check for loops if the rest of the schema is valid
        // because we rely on all link types being defined
        check_for_embedded_objects_loop(*this, exceptions);

        if (validation_mode & SchemaValidationMode::RejectEmbeddedOrphans) {
            auto orphans = this->get_embedded_object_orphans();
            for (auto& name : orphans) {
                exceptions.push_back(util::format(
                    "Embedded object '%1' is unreachable by any link path from top level objects.", name));
            }
        }
    }

    if (exceptions.size()) {
        throw SchemaValidationException(exceptions);
    }
}

static void compare(ObjectSchema const& existing_schema, ObjectSchema const& target_schema,
                    std::vector<SchemaChange>& changes)
{
    for (auto& current_prop : existing_schema.persisted_properties) {
        auto target_prop = target_schema.property_for_name(current_prop.name);

        if (!target_prop) {
            changes.emplace_back(schema_change::RemoveProperty{&existing_schema, &current_prop});
            continue;
        }
        if (target_schema.property_is_computed(*target_prop)) {
            changes.emplace_back(schema_change::RemoveProperty{&existing_schema, &current_prop});
            continue;
        }
        if (current_prop.type != target_prop->type || current_prop.object_type != target_prop->object_type ||
            is_array(current_prop.type) != is_array(target_prop->type) ||
            is_set(current_prop.type) != is_set(target_prop->type) ||
            is_dictionary(current_prop.type) != is_dictionary(target_prop->type)) {

            changes.emplace_back(schema_change::ChangePropertyType{&existing_schema, &current_prop, target_prop});
            continue;
        }
        if (is_nullable(current_prop.type) != is_nullable(target_prop->type)) {
            if (is_nullable(current_prop.type))
                changes.emplace_back(schema_change::MakePropertyRequired{&existing_schema, &current_prop});
            else
                changes.emplace_back(schema_change::MakePropertyNullable{&existing_schema, &current_prop});
        }
        if (target_prop->requires_index()) {
            if (!current_prop.is_indexed)
                changes.emplace_back(schema_change::AddIndex{&existing_schema, &current_prop});
        }
        else if (current_prop.requires_index()) {
            changes.emplace_back(schema_change::RemoveIndex{&existing_schema, &current_prop});
        }
    }

    for (auto& target_prop : target_schema.persisted_properties) {
        if (!existing_schema.property_for_name(target_prop.name)) {
            changes.emplace_back(schema_change::AddProperty{&existing_schema, &target_prop});
        }
    }

    if (existing_schema.primary_key != target_schema.primary_key) {
        changes.emplace_back(schema_change::ChangePrimaryKey{&existing_schema, target_schema.primary_key_property()});
    }
}

template <typename T, typename U, typename Func>
void Schema::zip_matching(T&& a, U&& b, Func&& func) noexcept
{
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        auto& object_schema = a[i];
        auto& matching_schema = b[j];
        int cmp = object_schema.name.compare(matching_schema.name);
        if (cmp == 0) {
            func(&object_schema, &matching_schema);
            ++i;
            ++j;
        }
        else if (cmp < 0) {
            func(&object_schema, nullptr);
            ++i;
        }
        else {
            func(nullptr, &matching_schema);
            ++j;
        }
    }
    for (; i < a.size(); ++i)
        func(&a[i], nullptr);
    for (; j < b.size(); ++j)
        func(nullptr, &b[j]);
}

std::vector<SchemaChange> Schema::compare(Schema const& target_schema, bool include_table_removals) const
{
    std::vector<SchemaChange> changes;

    // Add missing tables
    zip_matching(target_schema, *this, [&](const ObjectSchema* target, const ObjectSchema* existing) {
        if (target && !existing) {
            changes.emplace_back(schema_change::AddTable{target});
        }
        else if (existing && !target) {
            if (include_table_removals)
                changes.emplace_back(schema_change::RemoveTable{existing});
        }
        else if (existing->is_embedded != target->is_embedded)
            changes.emplace_back(schema_change::ChangeTableType{target});
    });

    // Modify columns
    zip_matching(target_schema, *this, [&](const ObjectSchema* target, const ObjectSchema* existing) {
        if (target && existing)
            ::compare(*existing, *target, changes);
        else if (target) {
            // Target is a new table -- add all properties
            changes.emplace_back(schema_change::AddInitialProperties{target});
        }
        // nothing for tables in existing but not target
    });
    return changes;
}

void Schema::copy_keys_from(realm::Schema const& other) noexcept
{
    zip_matching(*this, other, [&](ObjectSchema* existing, const ObjectSchema* other) {
        if (!existing || !other)
            return;

        existing->table_key = other->table_key;
        for (auto& current_prop : other->persisted_properties) {
            auto target_prop = existing->property_for_name(current_prop.name);
            if (target_prop) {
                target_prop->column_key = current_prop.column_key;
            }
        }
    });
}

namespace realm {
bool operator==(SchemaChange const& lft, SchemaChange const& rgt) noexcept
{
    if (lft.m_kind != rgt.m_kind)
        return false;

    using namespace schema_change;
    struct Visitor {
        SchemaChange const& value;

#define REALM_SC_COMPARE(type, ...)                                                                                  \
    bool operator()(type rgt) const                                                                                  \
    {                                                                                                                \
        auto cmp = [](auto&& v) {                                                                                    \
            return std::tie(__VA_ARGS__);                                                                            \
        };                                                                                                           \
        return cmp(value.type) == cmp(rgt);                                                                          \
    }

        REALM_SC_COMPARE(AddIndex, v.object, v.property)
        REALM_SC_COMPARE(AddProperty, v.object, v.property)
        REALM_SC_COMPARE(AddInitialProperties, v.object)
        REALM_SC_COMPARE(AddTable, v.object)
        REALM_SC_COMPARE(RemoveTable, v.object)
        REALM_SC_COMPARE(ChangeTableType, v.object)
        REALM_SC_COMPARE(ChangePrimaryKey, v.object, v.property)
        REALM_SC_COMPARE(ChangePropertyType, v.object, v.old_property, v.new_property)
        REALM_SC_COMPARE(MakePropertyNullable, v.object, v.property)
        REALM_SC_COMPARE(MakePropertyRequired, v.object, v.property)
        REALM_SC_COMPARE(RemoveIndex, v.object, v.property)
        REALM_SC_COMPARE(RemoveProperty, v.object, v.property)

#undef REALM_SC_COMPARE
    } visitor{lft};
    return rgt.visit(visitor);
}
} // namespace realm
