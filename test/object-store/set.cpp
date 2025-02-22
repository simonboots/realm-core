#include "catch2/catch.hpp"

#include "util/test_file.hpp"
#include "util/index_helpers.hpp"

#include <realm/object-store/binding_context.hpp>
#include <realm/object-store/set.hpp>
#include <realm/object-store/object.hpp>
#include <realm/object-store/object_schema.hpp>
#include <realm/object-store/property.hpp>
#include <realm/object-store/results.hpp>
#include <realm/object-store/schema.hpp>

#include <realm/object-store/impl/realm_coordinator.hpp>
#include <realm/object-store/impl/object_accessor_impl.hpp>

#include <realm/version.hpp>
#include <realm/db.hpp>

using namespace realm;

TEST_CASE("set") {
    InMemoryTestFile config;
    config.automatic_change_notifications = false;
    auto r = Realm::get_shared_realm(config);

    r->update_schema({
        {"table",
         {{"int_set", PropertyType::Set | PropertyType::Int},
          {"decimal_set", PropertyType::Set | PropertyType::Decimal | PropertyType::Nullable},
          {"link_set", PropertyType::Set | PropertyType::Object, "table2"}}},
        {"table2", {{"id", PropertyType::Int, Property::IsPrimary{true}}}},
        {"other_table",
         {{"int_set", PropertyType::Set | PropertyType::Int},
          {"link_set", PropertyType::Set | PropertyType::Object, "other_table2"}}},
        {"other_table2", {{"id", PropertyType::Int, Property::IsPrimary{true}}}},
    });

    auto& coordinator = *_impl::RealmCoordinator::get_coordinator(config.path);
    static_cast<void>(coordinator);

    auto table = r->read_group().get_table("class_table");
    auto table2 = r->read_group().get_table("class_table2");
    auto other_table = r->read_group().get_table("class_table");
    auto other_table2 = r->read_group().get_table("class_table2");

    ColKey col_int_set = table->get_column_key("int_set");
    ColKey col_decimal_set = table->get_column_key("decimal_set");

    ColKey col_link_set = table->get_column_key("link_set");
    ColKey col_link_obj_id = table2->get_column_key("id");
    ColKey other_col_link_set = table->get_column_key("link_set");

    auto write = [&](auto&& f) {
        r->begin_transaction();
        if constexpr (std::is_void_v<decltype(f())>) {
            f();
            r->commit_transaction();
            advance_and_notify(*r);
        }
        else {
            auto result = f();
            r->commit_transaction();
            advance_and_notify(*r);
            return result;
        }
    };

    auto obj = write([&] {
        return table->create_object();
    });

    auto other_obj = write([&] {
        return other_table->create_object();
    });

    SECTION("basics") {
        object_store::Set set{r, obj, col_int_set};

        write([&]() {
            CHECK(set.insert(123).second);
            CHECK(set.insert(456).second);
            CHECK(set.insert(0).second);
            CHECK(set.insert_any(-1).second);
            CHECK(!set.insert(456).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 4);
        CHECK(set.find(-1) == 0);
        CHECK(set.find(0) == 1);
        CHECK(set.get_any(2) == Mixed(123));
        CHECK(set.find_any(456) == 3);
        CHECK(set.find(999) == size_t(-1));

        write([&]() {
            CHECK(set.remove(123).second);
            CHECK(!set.remove(123).second);
            CHECK(set.remove_any(-1).second);
        });

        CHECK(set.size() == 2);

        write([&]() {
            obj.remove();
        });
        CHECK(!set.is_valid());
    }

    SECTION("nullable decimal") {
        object_store::Set set{r, obj, col_decimal_set};
        auto results = set.as_results();

        write([&]() {
            CHECK(set.insert(Decimal128(5)).second);
            CHECK(set.insert(Decimal128(realm::null())).second);
            CHECK(set.insert(Decimal128(7)).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 3);
        CHECK(results.index_of(Decimal128(realm::null())) == 0);
        auto sorted = results.sort({{"self", false}});
        CHECK(sorted.index_of(Decimal128(realm::null())) == 2);
    }

    SECTION("objects / links") {
        object_store::Set set{r, obj, col_link_set};

        Obj target1, target2, target3;
        write([&]() {
            target1 = table2->create_object_with_primary_key(123);
            target2 = table2->create_object_with_primary_key(456);
            target3 = table2->create_object_with_primary_key(789);
        });

        write([&]() {
            CHECK(set.insert(target1).second);
            CHECK(!set.insert(target1).second);
            CHECK(set.insert(target2).second);
            CHECK(set.insert(target3).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 3);

        CHECK(set.find(target1) != size_t(-1));
        CHECK(set.find(target2) != size_t(-1));
        CHECK(set.find(target3) != size_t(-1));

        write([&]() {
            target2.invalidate();
        });

        // Invalidating the object changes the reported size of the set.
        CHECK(set.size() == 2);

        CHECK_THROWS(set.find(target2));

        // Resurrect the tombstone of target2.
        write([&]() {
            target2 = table2->create_object_with_primary_key(456);
        });
        CHECK(set.find(target2));
        CHECK(set.size() == 3);
    }

    SECTION("max / min / sum / avg") {
        object_store::Set set{r, obj, col_int_set};

        write([&]() {
            CHECK(set.insert(123).second);
            CHECK(set.insert(456).second);
            CHECK(set.insert(0).second);
            CHECK(set.insert(-1).second);
        });

        auto x = set.min();

        REQUIRE(set.is_valid());
        CHECK(set.sum(col_int_set) == 578);
        CHECK(set.min(col_int_set) == -1);
        CHECK(set.max(col_int_set) == 456);
        CHECK(set.average(col_int_set) == 144.5);
    }

    SECTION("add_notification_block()") {
        CollectionChangeSet change;
        object_store::Set link_set{r, obj, col_link_set};
        object_store::Set int_set{r, obj, col_int_set};

        auto require_change = [&] {
            auto token = link_set.add_notification_callback([&](CollectionChangeSet c, std::exception_ptr) {
                change = c;
            });
            advance_and_notify(*r);
            return token;
        };

        auto require_no_change = [&] {
            bool first = true;
            auto token =
                link_set.add_notification_callback([&, first](CollectionChangeSet, std::exception_ptr) mutable {
                    REQUIRE(first);
                    first = false;
                });
            advance_and_notify(*r);
            return token;
        };

        SECTION("modifying the set sends change notifications") {
            Obj target1, target2, target3;
            write([&]() {
                target1 = table2->create_object_with_primary_key(123);
                target2 = table2->create_object_with_primary_key(456);
                target3 = table2->create_object_with_primary_key(789);
            });

            auto token = require_change();

            write([&]() {
                CHECK(link_set.insert(target1).second);
                CHECK(!link_set.insert(target1).second);
                CHECK(link_set.insert(target2).second);
                CHECK(link_set.insert(target3).second);
            });

            write([&] {
                CHECK(link_set.size() == 3);
                REQUIRE(link_set.remove(target2).second);
            });
            CHECK(link_set.size() == 2);
            REQUIRE_INDICES(change.deletions, 1);
        }

        SECTION("modifying a different set doesn't send a change notification") {
            auto token = require_no_change();
            write([&] {
                CHECK(int_set.insert(123).second);
            });
        }

        SECTION("deleting the set sends change notification") {
            auto token = require_change();

            Obj target1, target2, target3;
            write([&]() {
                target1 = table2->create_object_with_primary_key(123);
                target2 = table2->create_object_with_primary_key(456);
                target3 = table2->create_object_with_primary_key(789);
            });

            write([&]() {
                CHECK(link_set.insert(target1).second);
                CHECK(!link_set.insert(target1).second);
                CHECK(link_set.insert(target2).second);
                CHECK(link_set.insert(target3).second);
            });

            write([&] {
                link_set.remove_all();
            });
            REQUIRE_INDICES(change.deletions, 0, 1, 2);

            // Should not resend delete all notification after another commit
            change = {};
            write([&] {
                table->create_object();
            });
            REQUIRE(change.empty());
        }

        SECTION("deleting an empty set sends a change notification") {
            auto token = require_change();
            REQUIRE(link_set.size() == 0);
            REQUIRE(!change.collection_root_was_deleted);

            write([&]() {
                obj.remove();
            });
            REQUIRE(change.deletions.empty());
            REQUIRE(change.collection_root_was_deleted);

            // Should not resend delete all notification after another commit
            change = {};
            write([&] {
                table->create_object();
            });
            REQUIRE(change.empty());
        }
    }

    SECTION("find(Query)") {
        object_store::Set set{r, obj, col_link_set};

        Obj target1, target2, target3;
        write([&]() {
            target1 = table2->create_object_with_primary_key(123);
            target2 = table2->create_object_with_primary_key(456);
            target3 = table2->create_object_with_primary_key(789);
        });

        write([&]() {
            CHECK(set.insert(target1).second);
            CHECK(!set.insert(target1).second);
            CHECK(set.insert(target2).second);
            CHECK(set.insert(target3).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 3);

        SECTION("returns index in set for boxed Object") {
            REQUIRE(set.find(std::move(table2->where().equal(col_link_obj_id, 123))) == 0);
            REQUIRE(set.find(std::move(table2->where().equal(col_link_obj_id, 456))) == 1);
            REQUIRE(set.find(std::move(table2->where().equal(col_link_obj_id, 789))) == 2);
        }
    }

    SECTION("is_superset_of") {
        object_store::Set set{r, obj, col_link_set};
        object_store::Set set2{r, other_obj, other_col_link_set};

        std::vector<Obj> targets;
        write([&]() {
            targets.push_back(table2->create_object_with_primary_key(123));
            targets.push_back(table2->create_object_with_primary_key(456));
            targets.push_back(table2->create_object_with_primary_key(789));
            targets.push_back(table2->create_object_with_primary_key(321));
            targets.push_back(table2->create_object_with_primary_key(654));
            targets.push_back(table2->create_object_with_primary_key(987));
        });

        write([&]() {
            for (auto& obj : targets) {
                CHECK(set.insert(obj).second);
            }
            CHECK(set2.insert(targets[0]).second);
            CHECK(set2.insert(targets[1]).second);
            CHECK(set2.insert(targets[2]).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 6);
        REQUIRE(set2.is_valid());
        CHECK(set2.size() == 3);

        SECTION("set2 is a subset of set") {
            REQUIRE(set2.is_subset_of(set));
            REQUIRE_FALSE(set.is_subset_of(set2));
        }
    }

    SECTION("intersects") {
        object_store::Set set{r, obj, col_link_set};
        object_store::Set set2{r, other_obj, other_col_link_set};

        std::vector<Obj> targets;
        write([&]() {
            targets.push_back(table2->create_object_with_primary_key(123));
            targets.push_back(table2->create_object_with_primary_key(456));
            targets.push_back(table2->create_object_with_primary_key(789));
            targets.push_back(table2->create_object_with_primary_key(321));
            targets.push_back(table2->create_object_with_primary_key(654));
            targets.push_back(table2->create_object_with_primary_key(987));
        });

        std::vector<Obj> other_targets;
        write([&]() {
            other_targets.push_back(other_table2->create_object_with_primary_key(111));
            other_targets.push_back(other_table2->create_object_with_primary_key(222));
            other_targets.push_back(other_table2->create_object_with_primary_key(333));
        });

        write([&]() {
            for (auto& obj : targets) {
                CHECK(set.insert(obj).second);
            }
            for (auto& obj : other_targets) {
                CHECK(set2.insert(obj).second);
            }
            CHECK(set2.insert(targets[0]).second);
            CHECK(set2.insert(targets[1]).second);
            CHECK(set2.insert(targets[2]).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 6);
        REQUIRE(set2.is_valid());
        CHECK(set2.size() == 6);


        SECTION("set2 intersects set") {
            // (123, 456, 789, (321, 654, 987), 111, 222, 333)
            REQUIRE(set2.intersects(set));
            REQUIRE(set.intersects(set2));
            write([&]() {
                set2.remove(targets[0]);
                set2.remove(targets[1]);
                set2.remove(targets[2]);
            });
            // (123, 456, 789, (), 111, 222, 333)
            REQUIRE_FALSE(set2.intersects(set));
        }
    }

    SECTION("assign intersection") {
        object_store::Set set{r, obj, col_link_set};
        object_store::Set set2{r, other_obj, other_col_link_set};

        std::vector<Obj> targets;
        write([&]() {
            targets.push_back(table2->create_object_with_primary_key(123));
            targets.push_back(table2->create_object_with_primary_key(456));
            targets.push_back(table2->create_object_with_primary_key(789));
            targets.push_back(table2->create_object_with_primary_key(321));
            targets.push_back(table2->create_object_with_primary_key(654));
            targets.push_back(table2->create_object_with_primary_key(987));
        });

        std::vector<Obj> other_targets;
        write([&]() {
            other_targets.push_back(other_table2->create_object_with_primary_key(111));
            other_targets.push_back(other_table2->create_object_with_primary_key(222));
            other_targets.push_back(other_table2->create_object_with_primary_key(333));
        });

        write([&]() {
            for (auto& obj : targets) {
                CHECK(set.insert(obj).second);
            }
            for (auto& obj : other_targets) {
                CHECK(set2.insert(obj).second);
            }
            CHECK(set2.insert(targets[0]).second);
            CHECK(set2.insert(targets[1]).second);
            CHECK(set2.insert(targets[2]).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 6);
        REQUIRE(set2.is_valid());
        CHECK(set2.size() == 6);

        SECTION("set2 intersects set") {
            // (123, 456, 789, (321, 654, 987), 111, 222, 333)
            write([&] {
                set2.assign_intersection(set);
            });
            CHECK(set2.size() == 3);
        }
    }

    SECTION("assign union") {
        object_store::Set set{r, obj, col_link_set};
        object_store::Set set2{r, other_obj, other_col_link_set};

        std::vector<Obj> targets;
        write([&]() {
            targets.push_back(table2->create_object_with_primary_key(123));
            targets.push_back(table2->create_object_with_primary_key(456));
            targets.push_back(table2->create_object_with_primary_key(789));
            targets.push_back(table2->create_object_with_primary_key(321));
            targets.push_back(table2->create_object_with_primary_key(654));
            targets.push_back(table2->create_object_with_primary_key(987));
        });

        std::vector<Obj> other_targets;
        write([&]() {
            other_targets.push_back(other_table2->create_object_with_primary_key(111));
            other_targets.push_back(other_table2->create_object_with_primary_key(222));
            other_targets.push_back(other_table2->create_object_with_primary_key(333));
        });

        write([&]() {
            for (auto& obj : targets) {
                CHECK(set.insert(obj).second);
            }
            for (auto& obj : other_targets) {
                CHECK(set2.insert(obj).second);
            }
            CHECK(set2.insert(targets[0]).second);
            CHECK(set2.insert(targets[1]).second);
            CHECK(set2.insert(targets[2]).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 6);
        REQUIRE(set2.is_valid());
        CHECK(set2.size() == 6);

        SECTION("set2 intersects set") {
            // (123, 456, 789, (321, 654, 987), 111, 222, 333)
            write([&] {
                set2.assign_union(set);
            });
            CHECK(set2.size() == 9);
        }
    }

    SECTION("assign difference") {
        object_store::Set set{r, obj, col_link_set};
        object_store::Set set2{r, other_obj, other_col_link_set};

        std::vector<Obj> targets;
        write([&]() {
            targets.push_back(table2->create_object_with_primary_key(123));
            targets.push_back(table2->create_object_with_primary_key(456));
            targets.push_back(table2->create_object_with_primary_key(789));
            targets.push_back(table2->create_object_with_primary_key(321));
            targets.push_back(table2->create_object_with_primary_key(654));
            targets.push_back(table2->create_object_with_primary_key(987));
        });

        std::vector<Obj> other_targets;
        write([&]() {
            other_targets.push_back(other_table2->create_object_with_primary_key(111));
            other_targets.push_back(other_table2->create_object_with_primary_key(222));
            other_targets.push_back(other_table2->create_object_with_primary_key(333));
        });

        write([&]() {
            for (auto& obj : targets) {
                CHECK(set.insert(obj).second);
            }
            for (auto& obj : other_targets) {
                CHECK(set2.insert(obj).second);
            }
            CHECK(set2.insert(targets[0]).second);
            CHECK(set2.insert(targets[1]).second);
            CHECK(set2.insert(targets[2]).second);
        });

        REQUIRE(set.is_valid());
        CHECK(set.size() == 6);
        REQUIRE(set2.is_valid());
        CHECK(set2.size() == 6);

        SECTION("set2 intersects set") {
            // (123, 456, 789, (321, 654, 987), 111, 222, 333)
            write([&] {
                set2.assign_difference(set);
            });
            CHECK(set2.size() == 3);
        }
    }
}
