/*************************************************************************
 *
 * Copyright 2016 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **************************************************************************/

#include <cstdio>
#include <iomanip>

#ifdef REALM_DEBUG
#include <iostream>
#endif

#include <realm/exceptions.hpp>
#include <realm/index_string.hpp>
#include <realm/table.hpp>
#include <realm/timestamp.hpp>
#include <realm/column_integer.hpp>

using namespace realm;
using namespace realm::util;

namespace {

void get_child(Array& parent, size_t child_ref_ndx, Array& child) noexcept
{
    ref_type child_ref = parent.get_as_ref(child_ref_ndx);
    child.init_from_ref(child_ref);
    child.set_parent(&parent, child_ref_ndx);
}

} // anonymous namespace

DataType ClusterColumn::get_data_type() const
{
    const Table* table = m_cluster_tree->get_owning_table();
    return table->get_column_type(m_column_key);
}

bool ClusterColumn::is_nullable() const
{
    return m_column_key.get_attrs().test(col_attr_Nullable);
}

StringData ClusterColumn::get_index_data(ObjKey key, StringConversionBuffer& buffer) const
{
    const Obj obj{m_cluster_tree->get(key)};
    DataType type = get_data_type();

    if (type == type_Int) {
        if (is_nullable()) {
            GetIndexData<Optional<int64_t>> stringifier;
            return stringifier.get_index_data(obj.get<Optional<int64_t>>(m_column_key), buffer);
        }
        else {
            GetIndexData<int64_t> stringifier;
            return stringifier.get_index_data(obj.get<int64_t>(m_column_key), buffer);
        }
    }
    else if (type == type_Bool) {
        if (is_nullable()) {
            GetIndexData<Optional<bool>> stringifier;
            return stringifier.get_index_data(obj.get<Optional<bool>>(m_column_key), buffer);
        }
        else {
            GetIndexData<bool> stringifier;
            return stringifier.get_index_data(obj.get<bool>(m_column_key), buffer);
        }
    }
    else if (type == type_String) {
        GetIndexData<String> stringifier;
        return stringifier.get_index_data(obj.get<String>(m_column_key), buffer);
    }
    else if (type == type_Timestamp) {
        GetIndexData<Timestamp> stringifier;
        return stringifier.get_index_data(obj.get<Timestamp>(m_column_key), buffer);
    }
    else if (type == type_ObjectId) {
        if (is_nullable()) {
            GetIndexData<Optional<ObjectId>> stringifier;
            return stringifier.get_index_data(obj.get<Optional<ObjectId>>(m_column_key), buffer);
        }
        else {
            GetIndexData<ObjectId> stringifier;
            return stringifier.get_index_data(obj.get<ObjectId>(m_column_key), buffer);
        }
    }
    else if (type == type_Mixed) {
        GetIndexData<Mixed> stringifier;
        return stringifier.get_index_data(obj.get<Mixed>(m_column_key), buffer);
    }
    else if (type == type_UUID) {
        if (is_nullable()) {
            GetIndexData<Optional<UUID>> stringifier;
            return stringifier.get_index_data(obj.get<Optional<UUID>>(m_column_key), buffer);
        }
        else {
            GetIndexData<UUID> stringifier;
            return stringifier.get_index_data(obj.get<UUID>(m_column_key), buffer);
        }
    }
    // It should not be possible to reach this line through public Core API
    REALM_ASSERT_RELEASE(false);
    return {};
}

namespace realm {
StringData GetIndexData<Timestamp>::get_index_data(const Timestamp& dt, StringConversionBuffer& buffer)
{
    if (dt.is_null())
        return null{};

    int64_t s = dt.get_seconds();
    int32_t ns = dt.get_nanoseconds();
    constexpr size_t index_size = sizeof(s) + sizeof(ns);
    static_assert(index_size <= string_conversion_buffer_size, "Index string conversion buffer too small");
    const char* s_buf = reinterpret_cast<const char*>(&s);
    const char* ns_buf = reinterpret_cast<const char*>(&ns);
    realm::safe_copy_n(s_buf, sizeof(s), buffer.data());
    realm::safe_copy_n(ns_buf, sizeof(ns), buffer.data() + sizeof(s));
    return StringData{buffer.data(), index_size};
}

template <>
int64_t IndexArray::from_list<index_FindFirst>(StringData value, InternalFindResult& /* result_ref */,
                                               const IntegerColumn& key_values, const ClusterColumn& column) const
{
    SortedListComparator slc(column);

    IntegerColumn::const_iterator it_end = key_values.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(key_values.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return null_key.value;

    int64_t first_key_value = *lower;

    // The buffer is needed when for when this is an integer index.
    StringConversionBuffer buffer;
    StringData str = column.get_index_data(ObjKey(first_key_value), buffer);
    if (str != value)
        return null_key.value;

    return first_key_value;
}

template <>
int64_t IndexArray::from_list<index_Count>(StringData value, InternalFindResult& /* result_ref */,
                                           const IntegerColumn& key_values, const ClusterColumn& column) const
{
    SortedListComparator slc(column);

    IntegerColumn::const_iterator it_end = key_values.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(key_values.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return 0;

    int64_t first_key_value = *lower;

    // The buffer is needed when for when this is an integer index.
    StringConversionBuffer buffer;
    StringData str = column.get_index_data(ObjKey(first_key_value), buffer);
    if (str != value)
        return 0;

    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, value, slc);
    int64_t cnt = upper - lower;

    return cnt;
}

template <>
int64_t IndexArray::from_list<index_FindAll_nocopy>(StringData value, InternalFindResult& result_ref,
                                                    const IntegerColumn& key_values,
                                                    const ClusterColumn& column) const
{
    SortedListComparator slc(column);
    IntegerColumn::const_iterator it_end = key_values.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(key_values.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return size_t(FindRes_not_found);

    ObjKey first_key = ObjKey(*lower);

    // The buffer is needed when for when this is an integer index.
    StringConversionBuffer buffer;
    StringData str = column.get_index_data(first_key, buffer);
    if (str != value)
        return size_t(FindRes_not_found);

    // Optimization: check the last entry before trying upper bound.
    IntegerColumn::const_iterator upper = it_end;
    --upper;
    // Single result if upper matches lower
    if (upper == lower) {
        result_ref.payload = *lower;
        return size_t(FindRes_single);
    }

    // Check string value at upper, if equal return matches in (lower, upper]
    ObjKey last_key = ObjKey(*upper);
    str = column.get_index_data(last_key, buffer);
    if (str == value) {
        result_ref.payload = from_ref(key_values.get_ref());
        result_ref.start_ndx = lower.get_position();
        result_ref.end_ndx = upper.get_position() + 1; // one past last match
        return size_t(FindRes_column);
    }

    // Last result is not equal, find the upper bound of the range of results.
    // Note that we are passing upper which is cend() - 1 here as we already
    // checked the last item manually.
    upper = std::upper_bound(lower, upper, value, slc);

    result_ref.payload = from_ref(key_values.get_ref());
    result_ref.start_ndx = lower.get_position();
    result_ref.end_ndx = upper.get_position();
    return size_t(FindRes_column);
}


template <IndexMethod method>
int64_t IndexArray::index_string(StringData value, InternalFindResult& result_ref, const ClusterColumn& column) const
{
    // Return`realm::not_found`, or an index to the (any) match
    constexpr bool first(method == index_FindFirst);
    // Return 0, or the number of items that match the specified `value`
    constexpr bool get_count(method == index_Count);
    // Same as `index_FindAll` but does not copy matching rows into `column`
    // returns FindRes_not_found if there are no matches
    // returns FindRes_single and the row index (literal) in result_ref.payload
    // or returns FindRes_column and the reference to a column of duplicates in
    // result_ref.result with the results in the bounds start_ndx, and end_ndx
    constexpr bool allnocopy(method == index_FindAll_nocopy);

    constexpr int64_t local_not_found = allnocopy ? int64_t(FindRes_not_found) : first ? null_key.value : 0;

    const char* data = m_data;
    const char* header;
    uint_least8_t width = m_width;
    bool is_inner_node = m_is_inner_bptree_node;
    typedef StringIndex::key_type key_type;
    size_t stringoffset = 0;

    // Create 4 byte index key
    key_type key = StringIndex::create_key(value, stringoffset);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return local_not_found;

        // Get entry under key
        size_t pos_refs = pos + 1; // first entry in refs points to offsets
        uint64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            header = m_alloc.translate(to_ref(ref));
            data = get_data_from_header(header);
            width = get_width_from_header(header);
            is_inner_node = get_is_inner_bptree_node_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return local_not_found;

        // Literal row index (tagged)
        if (ref & 1) {
            int64_t key_value = int64_t(ref >> 1);

            // The buffer is needed when for when this is an integer index.
            StringConversionBuffer buffer;
            StringData str = column.get_index_data(ObjKey(key_value), buffer);
            if (str == value) {
                result_ref.payload = key_value;
                return first ? key_value : get_count ? 1 : FindRes_single;
            }
            return local_not_found;
        }

        const char* sub_header = m_alloc.translate(ref_type(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, ref_type(ref));
            return from_list<method>(value, result_ref, sub, column);
        }

        // Recurse into sub-index;
        header = sub_header;
        data = get_data_from_header(header);
        width = get_width_from_header(header);
        is_inner_node = get_is_inner_bptree_node_from_header(header);

        // Go to next key part of the string. If the offset exceeds the string length, the key will be 0
        stringoffset += 4;

        // Update 4 byte index key
        key = StringIndex::create_key(value, stringoffset);
    }
}


void IndexArray::from_list_all_ins(StringData upper_value, std::vector<ObjKey>& result, const IntegerColumn& rows,
                                   const ClusterColumn& column) const
{
    // The buffer is needed when for when this is an integer index.
    StringConversionBuffer buffer;

    // optimization for the most common case, where all the strings under a given subindex are equal
    StringData first_str = column.get_index_data(ObjKey(*rows.cbegin()), buffer);
    StringData last_str = column.get_index_data(ObjKey(*(rows.cend() - 1)), buffer);
    if (first_str == last_str) {
        auto first_str_upper = case_map(first_str, true);
        if (first_str_upper != upper_value) {
            return;
        }

        size_t sz = result.size() + rows.size();
        result.reserve(sz);
        for (IntegerColumn::const_iterator it = rows.cbegin(); it != rows.cend(); ++it) {
            result.push_back(ObjKey(*it));
        }
        return;
    }

    // special case for very long strings, where they might have a common prefix and end up in the
    // same subindex column, but still not be identical
    for (IntegerColumn::const_iterator it = rows.cbegin(); it != rows.cend(); ++it) {
        ObjKey key = ObjKey(*it);
        StringData str = column.get_index_data(key, buffer);
        auto upper_str = case_map(str, true);
        if (upper_str == upper_value) {
            result.push_back(key);
        }
    }

    return;
}


void IndexArray::from_list_all(StringData value, std::vector<ObjKey>& result, const IntegerColumn& rows,
                               const ClusterColumn& column) const
{
    SortedListComparator slc(column);

    IntegerColumn::const_iterator it_end = rows.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(rows.cbegin(), it_end, value, slc);
    if (lower == it_end)
        return;

    ObjKey key = ObjKey(*lower);

    // The buffer is needed when for when this is an integer index.
    StringConversionBuffer buffer;
    StringData str = column.get_index_data(key, buffer);
    if (str != value)
        return;

    IntegerColumn::const_iterator upper = std::upper_bound(lower, it_end, value, slc);

    // Copy all matches into result column
    size_t sz = result.size() + (upper - lower);
    result.reserve(sz);
    for (IntegerColumn::const_iterator it = lower; it != upper; ++it) {
        result.push_back(ObjKey(*it));
    }

    return;
}


namespace {

// Helper functions for SearchList (index_string_all_ins) for generating permutations of index keys

// replicates the 4 least significant bits each times 8
// eg: abcd -> aaaaaaaabbbbbbbbccccccccdddddddd
int32_t replicate_4_lsb_x8(int32_t i) {
    REALM_ASSERT_DEBUG(0 <= i && i <= 15);
    i *= 0x204081;
    i &= 0x1010101;
    i *= 0xff;
    return i;
}

int32_t select_from_mask(int32_t a, int32_t b, int32_t mask) {
    return a ^ ((a ^ b) & mask);
}

// Given upper and lower keys: "ABCD" and "abcd", the 4 LSBs in the permutation argument determine the
// final key:
// Permutation 0  = "ABCD"
// Permutation 1  = "ABCd"
// Permutation 8  = "aBCD"
// Permutation 15 = "abcd"
using key_type = StringIndex::key_type;
key_type generate_key(key_type upper, key_type lower, int permutation) {
    return select_from_mask(upper, lower, replicate_4_lsb_x8(permutation));
}


// Helper structure for IndexArray::index_string_all_ins to generate and keep track of search key permutations,
// when traversing the trees.
struct SearchList {
    struct Item {
        const char* header;
        size_t string_offset;
        key_type key;
    };

    SearchList(const util::Optional<std::string>& upper_value, const util::Optional<std::string>& lower_value)
        : m_upper_value(upper_value)
        , m_lower_value(lower_value)
    {
        m_keys_seen.reserve(num_permutations);
    }

    // Add all unique keys for this level to the internal work stack
    void add_all_for_level(const char* header, size_t string_offset)
    {
        m_keys_seen.clear();
        const key_type upper_key = StringIndex::create_key(m_upper_value, string_offset);
        const key_type lower_key = StringIndex::create_key(m_lower_value, string_offset);
        for (int p = 0; p < num_permutations; ++p) {
            // FIXME: This might still be incorrect due to multi-byte unicode characters (crossing the 4 byte key
            // size) being combined incorrectly.
            const key_type key = generate_key(upper_key, lower_key, p);
            const bool new_key = std::find(m_keys_seen.cbegin(), m_keys_seen.cend(), key) == m_keys_seen.cend();
            if (new_key) {
                m_keys_seen.push_back(key);
                add_next(header, string_offset, key);
            }
        }
    }

    bool empty() const
    {
        return m_items.empty();
    }

    Item get_next()
    {
        Item item = m_items.back();
        m_items.pop_back();
        return item;
    }

    // Add a single entry to the internal work stack. Used to traverse the inner trees (same key)
    void add_next(const char* header, size_t string_offset, key_type key)
    {
        m_items.push_back({header, string_offset, key});
    }

private:
    static constexpr int num_permutations = 1 << sizeof(key_type); // 4 bytes gives up to 16 search keys

    std::vector<Item> m_items;

    const util::Optional<std::string> m_upper_value;
    const util::Optional<std::string> m_lower_value;

    std::vector<key_type> m_keys_seen;
};


} // namespace


void IndexArray::index_string_all_ins(StringData value, std::vector<ObjKey>& result,
                                      const ClusterColumn& column) const
{
    if (value.is_null()) {
        // we can't use case_map on null strings because it currently returns an
        // empty string ("") in that case which is different than a null StringData
        return index_string_all(value, result, column);
    }

    const util::Optional<std::string> upper_value = case_map(value, true);
    const util::Optional<std::string> lower_value = case_map(value, false);
    SearchList search_list(upper_value, lower_value);

    const char* top_header = get_header_from_data(m_data);
    search_list.add_all_for_level(top_header, 0);

    while (!search_list.empty()) {
        SearchList::Item item = search_list.get_next();

        const char* const header = item.header;
        const size_t string_offset = item.string_offset;
        const key_type key = item.key;
        const char* const data = get_data_from_header(header);
        const uint_least8_t width = get_width_from_header(header);
        const bool is_inner_node = get_is_inner_bptree_node_from_header(header);

        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));

        // Find the position matching the key
        const char* const offsets_header = m_alloc.translate(offsets_ref);
        const char* const offsets_data = get_data_from_header(offsets_header);
        const size_t offsets_size = get_size_from_header(offsets_header);
        const size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            continue;

        // Get entry under key
        const size_t pos_refs = pos + 1; // first entry in refs points to offsets
        const uint64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            const char* const inner_header = m_alloc.translate(to_ref(ref));
            search_list.add_next(inner_header, string_offset, key);
            continue;
        }

        const key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            continue;

        // Literal row index (tagged)
        if (ref & 1) {
            ObjKey k(int64_t(ref >> 1));

            // The buffer is needed when for when this is an integer index.
            StringConversionBuffer buffer;
            const StringData str = column.get_index_data(k, buffer);
            const util::Optional<std::string> upper_str = case_map(str, true);
            if (upper_str == upper_value) {
                result.push_back(k);
            }
            continue;
        }

        const char* const sub_header = m_alloc.translate(ref_type(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, ref_type(ref));
            from_list_all_ins(upper_value, result, sub, column);
            continue;
        }

        // Recurse into sub-index;
        search_list.add_all_for_level(sub_header, string_offset + 4);
    }

    // sort the result and return a std::vector
    std::sort(result.begin(), result.end());
}


void IndexArray::index_string_all(StringData value, std::vector<ObjKey>& result, const ClusterColumn& column) const
{
    const char* data = m_data;
    const char* header;
    uint_least8_t width = m_width;
    bool is_inner_node = m_is_inner_bptree_node;
    size_t stringoffset = 0;

    // Create 4 byte index key
    key_type key = StringIndex::create_key(value, stringoffset);

    for (;;) {
        // Get subnode table
        ref_type offsets_ref = to_ref(get_direct(data, width, 0));

        // Find the position matching the key
        const char* offsets_header = m_alloc.translate(offsets_ref);
        const char* offsets_data = get_data_from_header(offsets_header);
        size_t offsets_size = get_size_from_header(offsets_header);
        size_t pos = ::lower_bound<32>(offsets_data, offsets_size, key); // keys are always 32 bits wide

        // If key is outside range, we know there can be no match
        if (pos == offsets_size)
            return;

        // Get entry under key
        size_t pos_refs = pos + 1; // first entry in refs points to offsets
        uint64_t ref = get_direct(data, width, pos_refs);

        if (is_inner_node) {
            // Set vars for next iteration
            header = m_alloc.translate(ref_type(ref));
            data = get_data_from_header(header);
            width = get_width_from_header(header);
            is_inner_node = get_is_inner_bptree_node_from_header(header);
            continue;
        }

        key_type stored_key = key_type(get_direct<32>(offsets_data, pos));

        if (stored_key != key)
            return;

        // Literal row index (tagged)
        if (ref & 1) {
            ObjKey k(int64_t(ref >> 1));

            // The buffer is needed when for when this is an integer index.
            StringConversionBuffer buffer;
            StringData str = column.get_index_data(k, buffer);
            if (str == value) {
                result.push_back(k);
                return;
            }
            return;
        }

        const char* sub_header = m_alloc.translate(ref_type(ref));
        const bool sub_isindex = get_context_flag_from_header(sub_header);

        // List of row indices with common prefix up to this point, in sorted order.
        if (!sub_isindex) {
            const IntegerColumn sub(m_alloc, ref_type(ref));
            return from_list_all(value, result, sub, column);
        }

        // Recurse into sub-index;
        header = sub_header;
        data = get_data_from_header(header);
        width = get_width_from_header(header);
        is_inner_node = get_is_inner_bptree_node_from_header(header);

        // Go to next key part of the string. If the offset exceeds the string length, the key will be 0
        stringoffset += 4;

        // Update 4 byte index key
        key = StringIndex::create_key(value, stringoffset);
    }
}


} // namespace realm

ObjKey IndexArray::index_string_find_first(StringData value, const ClusterColumn& column) const
{
    InternalFindResult unused;
    return ObjKey(index_string<index_FindFirst>(value, unused, column));
}


void IndexArray::index_string_find_all(std::vector<ObjKey>& result, StringData value, const ClusterColumn& column,
                                       bool case_insensitive) const
{
    if (case_insensitive) {
        index_string_all_ins(value, result, column);
    } else {
        index_string_all(value, result, column);
    }
}

FindRes IndexArray::index_string_find_all_no_copy(StringData value, const ClusterColumn& column,
                                                  InternalFindResult& result) const
{
    return static_cast<FindRes>(index_string<index_FindAll_nocopy>(value, result, column));
}

size_t IndexArray::index_string_count(StringData value, const ClusterColumn& column) const
{
    InternalFindResult unused;
    return to_size_t(index_string<index_Count>(value, unused, column));
}

IndexArray* StringIndex::create_node(Allocator& alloc, bool is_leaf)
{
    Array::Type type = is_leaf ? Array::type_HasRefs : Array::type_InnerBptreeNode;
    std::unique_ptr<IndexArray> top(new IndexArray(alloc)); // Throws
    top->create(type);                                      // Throws

    // Mark that this is part of index
    // (as opposed to columns under leaves)
    top->set_context_flag(true);

    // Add subcolumns for leaves
    Array values(alloc);
    values.create(Array::type_Normal);       // Throws
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit
    top->add(values.get_ref());              // first entry in refs points to offsets

    return top.release();
}

ref_type StringIndex::create_empty(Allocator& alloc)
{
    return StringIndex(ClusterColumn(nullptr, {}), alloc).get_ref(); // Throws
}

void StringIndex::set_target(const ClusterColumn& target_column) noexcept
{
    m_target_column = target_column;
}


StringIndex::key_type StringIndex::get_last_key() const
{
    Array offsets(m_array->get_alloc());
    get_child(*m_array, 0, offsets);
    return key_type(offsets.back());
}


void StringIndex::insert_with_offset(ObjKey obj_key, StringData value, size_t offset)
{
    // Create 4 byte index key
    key_type key = create_key(value, offset);
    TreeInsert(obj_key, key, offset, value); // Throws
}

void StringIndex::insert_to_existing_list_at_lower(ObjKey key, StringData value, IntegerColumn& list,
                                                   const IntegerColumnIterator& lower)
{
    SortedListComparator slc(m_target_column);
    // At this point there exists duplicates of this value, we need to
    // insert value beside it's duplicates so that rows are also sorted
    // in ascending order.
    IntegerColumn::const_iterator upper = std::upper_bound(lower, list.cend(), value, slc);
    // find insert position (the list has to be kept in sorted order)
    // In most cases the refs will be added to the end. So we test for that
    // first to see if we can avoid the binary search for insert position
    IntegerColumn::const_iterator last = upper - ptrdiff_t(1);
    int64_t last_key_value = *last;
    if (key.value >= last_key_value) {
        list.insert(upper.get_position(), key.value);
    }
    else {
        IntegerColumn::const_iterator inner_lower = std::lower_bound(lower, upper, key.value);
        list.insert(inner_lower.get_position(), key.value);
    }
}

void StringIndex::insert_to_existing_list(ObjKey key, StringData value, IntegerColumn& list)
{
    SortedListComparator slc(m_target_column);
    IntegerColumn::const_iterator it_end = list.cend();
    IntegerColumn::const_iterator lower = std::lower_bound(list.cbegin(), it_end, value, slc);

    if (lower == it_end) {
        // Not found and everything is less, just append it to the end.
        list.add(key.value);
    }
    else {
        ObjKey lower_key = ObjKey(*lower);
        StringConversionBuffer buffer; // Used when this is an IntegerIndex
        StringData lower_value = get(lower_key, buffer);

        if (lower_value != value) {
            list.insert(lower.get_position(), key.value);
        }
        else {
            // At this point there exists duplicates of this value, we need to
            // insert value beside it's duplicates so that rows are also sorted
            // in ascending order.
            insert_to_existing_list_at_lower(key, value, list, lower);
        }
    }
}


void StringIndex::insert_row_list(size_t ref, size_t offset, StringData value)
{
    REALM_ASSERT(!m_array->is_inner_bptree_node()); // only works in leaves

    // Create 4 byte index key
    key_type key = create_key(value, offset);

    // Get subnode table
    Allocator& alloc = m_array->get_alloc();
    Array values(alloc);
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size() + 1);

    size_t ins_pos = values.lower_bound_int(key);
    if (ins_pos == values.size()) {
        // When key is outside current range, we can just add it
        values.add(key);
        m_array->add(ref);
        return;
    }

#ifdef REALM_DEBUG // LCOV_EXCL_START ignore debug code
    // Since we only use this for moving existing values to new
    // subindexes, there should never be an existing match.
    key_type k = key_type(values.get(ins_pos));
    REALM_ASSERT(k != key);
#endif // LCOV_EXCL_STOP ignore debug code

    // If key is not present we add it at the correct location
    values.insert(ins_pos, key);
    m_array->insert(ins_pos + 1, ref);
}


void StringIndex::TreeInsert(ObjKey obj_key, key_type key, size_t offset, StringData value)
{
    NodeChange nc = do_insert(obj_key, key, offset, value);
    switch (nc.type) {
        case NodeChange::change_None:
            return;
        case NodeChange::change_InsertBefore: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.node_add_key(nc.ref1);
            new_node.node_add_key(get_ref());
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
        case NodeChange::change_InsertAfter: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.node_add_key(get_ref());
            new_node.node_add_key(nc.ref1);
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
        case NodeChange::change_Split: {
            StringIndex new_node(inner_node_tag(), m_array->get_alloc());
            new_node.node_add_key(nc.ref1);
            new_node.node_add_key(nc.ref2);
            m_array->init_from_ref(new_node.get_ref());
            m_array->update_parent();
            return;
        }
    }
    REALM_ASSERT(false); // LCOV_EXCL_LINE; internal Realm error
}


StringIndex::NodeChange StringIndex::do_insert(ObjKey obj_key, key_type key, size_t offset, StringData value)
{
    Allocator& alloc = m_array->get_alloc();
    if (m_array->is_inner_bptree_node()) {
        // Get subnode table
        Array keys(alloc);
        get_child(*m_array, 0, keys);
        REALM_ASSERT(m_array->size() == keys.size() + 1);

        // Find the subnode containing the item
        size_t node_ndx = keys.lower_bound_int(key);
        if (node_ndx == keys.size()) {
            // node can never be empty, so try to fit in last item
            node_ndx = keys.size() - 1;
        }

        // Get sublist
        size_t refs_ndx = node_ndx + 1; // first entry in refs points to offsets
        ref_type ref = m_array->get_as_ref(refs_ndx);
        StringIndex target(ref, m_array.get(), refs_ndx, m_target_column, alloc);

        // Insert item
        NodeChange nc = target.do_insert(obj_key, key, offset, value);
        if (nc.type == NodeChange::change_None) {
            // update keys
            key_type last_key = target.get_last_key();
            keys.set(node_ndx, last_key);
            return NodeChange::change_None; // no new nodes
        }

        if (nc.type == NodeChange::change_InsertAfter) {
            ++node_ndx;
            ++refs_ndx;
        }

        // If there is room, just update node directly
        if (keys.size() < REALM_MAX_BPNODE_SIZE) {
            if (nc.type == NodeChange::change_Split) {
                node_insert_split(node_ndx, nc.ref2);
            }
            else {
                node_insert(node_ndx, nc.ref1); // ::INSERT_BEFORE/AFTER
            }
            return NodeChange::change_None;
        }

        // Else create new node
        StringIndex new_node(inner_node_tag(), alloc);
        if (nc.type == NodeChange::change_Split) {
            // update offset for left node
            key_type last_key = target.get_last_key();
            keys.set(node_ndx, last_key);

            new_node.node_add_key(nc.ref2);
            ++node_ndx;
            ++refs_ndx;
        }
        else {
            new_node.node_add_key(nc.ref1);
        }

        switch (node_ndx) {
            case 0: // insert before
                return NodeChange(NodeChange::change_InsertBefore, new_node.get_ref());
            case REALM_MAX_BPNODE_SIZE: // insert after
                if (nc.type == NodeChange::change_Split)
                    return NodeChange(NodeChange::change_Split, get_ref(), new_node.get_ref());
                return NodeChange(NodeChange::change_InsertAfter, new_node.get_ref());
            default: // split
                // Move items after split to new node
                size_t len = m_array->size();
                for (size_t i = refs_ndx; i < len; ++i) {
                    ref_type ref_i = m_array->get_as_ref(i);
                    new_node.node_add_key(ref_i);
                }
                keys.truncate(node_ndx);
                m_array->truncate(refs_ndx);
                return NodeChange(NodeChange::change_Split, get_ref(), new_node.get_ref());
        }
    }
    else {
        // Is there room in the list?
        Array old_keys(alloc);
        get_child(*m_array, 0, old_keys);
        const size_t old_offsets_size = old_keys.size();
        REALM_ASSERT_EX(m_array->size() == old_offsets_size + 1, m_array->size(), old_offsets_size + 1);

        bool noextend = old_offsets_size >= REALM_MAX_BPNODE_SIZE;

        // See if we can fit entry into current leaf
        // Works if there is room or it can join existing entries
        if (leaf_insert(obj_key, key, offset, value, noextend))
            return NodeChange::change_None;

        // Create new list for item (a leaf)
        StringIndex new_list(m_target_column, alloc);

        new_list.leaf_insert(obj_key, key, offset, value);

        size_t ndx = old_keys.lower_bound_int(key);

        // insert before
        if (ndx == 0)
            return NodeChange(NodeChange::change_InsertBefore, new_list.get_ref());

        // insert after
        if (ndx == old_offsets_size)
            return NodeChange(NodeChange::change_InsertAfter, new_list.get_ref());

        // split
        Array new_keys(alloc);
        get_child(*new_list.m_array, 0, new_keys);
        // Move items after split to new list
        for (size_t i = ndx; i < old_offsets_size; ++i) {
            int64_t v2 = old_keys.get(i);
            int64_t v3 = m_array->get(i + 1);

            new_keys.add(v2);
            new_list.m_array->add(v3);
        }
        old_keys.truncate(ndx);
        m_array->truncate(ndx + 1);

        return NodeChange(NodeChange::change_Split, get_ref(), new_list.get_ref());
    }
}


void StringIndex::node_insert_split(size_t ndx, size_t new_ref)
{
    REALM_ASSERT(m_array->is_inner_bptree_node());
    REALM_ASSERT(new_ref);

    Allocator& alloc = m_array->get_alloc();
    Array offsets(alloc);
    get_child(*m_array, 0, offsets);

    REALM_ASSERT(m_array->size() == offsets.size() + 1);
    REALM_ASSERT(ndx < offsets.size());
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE);

    // Get sublists
    size_t refs_ndx = ndx + 1; // first entry in refs points to offsets
    ref_type orig_ref = m_array->get_as_ref(refs_ndx);
    StringIndex orig_col(orig_ref, m_array.get(), refs_ndx, m_target_column, alloc);
    StringIndex new_col(new_ref, nullptr, 0, m_target_column, alloc);

    // Update original key
    key_type last_key = orig_col.get_last_key();
    offsets.set(ndx, last_key);

    // Insert new ref
    key_type new_key = new_col.get_last_key();
    offsets.insert(ndx + 1, new_key);
    m_array->insert(ndx + 2, new_ref);
}


void StringIndex::node_insert(size_t ndx, size_t ref)
{
    REALM_ASSERT(ref);
    REALM_ASSERT(m_array->is_inner_bptree_node());

    Allocator& alloc = m_array->get_alloc();
    Array offsets(alloc);
    get_child(*m_array, 0, offsets);
    REALM_ASSERT(m_array->size() == offsets.size() + 1);

    REALM_ASSERT(ndx <= offsets.size());
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE);

    StringIndex col(ref, nullptr, 0, m_target_column, alloc);
    key_type last_key = col.get_last_key();

    offsets.insert(ndx, last_key);
    m_array->insert(ndx + 1, ref);
}


bool StringIndex::leaf_insert(ObjKey obj_key, key_type key, size_t offset, StringData value, bool noextend)
{
    REALM_ASSERT(!m_array->is_inner_bptree_node());

    // Get subnode table
    Allocator& alloc = m_array->get_alloc();
    Array keys(alloc);
    get_child(*m_array, 0, keys);
    REALM_ASSERT(m_array->size() == keys.size() + 1);

    size_t ins_pos = keys.lower_bound_int(key);
    if (ins_pos == keys.size()) {
        if (noextend)
            return false;

        // When key is outside current range, we can just add it
        keys.add(key);
        int64_t shifted = int64_t((uint64_t(obj_key.value) << 1) + 1); // shift to indicate literal
        m_array->add(shifted);
        return true;
    }

    size_t ins_pos_refs = ins_pos + 1; // first entry in refs points to offsets
    key_type k = key_type(keys.get(ins_pos));

    // If key is not present we add it at the correct location
    if (k != key) {
        if (noextend)
            return false;

        keys.insert(ins_pos, key);
        int64_t shifted = int64_t((uint64_t(obj_key.value) << 1) + 1); // shift to indicate literal
        m_array->insert(ins_pos_refs, shifted);
        return true;
    }

    // This leaf already has a slot for for the key

    uint64_t slot_value = uint64_t(m_array->get(ins_pos_refs));
    size_t suboffset = offset + s_index_key_length;

    // Single match (lowest bit set indicates literal row_ndx)
    if ((slot_value & 1) != 0) {
        ObjKey obj_key2 = ObjKey(int64_t(slot_value >> 1));
        // The buffer is needed for when this is an integer index.
        StringConversionBuffer buffer;
        StringData v2 = get(obj_key2, buffer);
        if (v2 == value) {
            // Strings are equal but this is not a list.
            // Create a list and add both rows.

            // convert to list (in sorted order)
            Array row_list(alloc);
            row_list.create(Array::type_Normal); // Throws
            row_list.add(obj_key < obj_key2 ? obj_key.value : obj_key2.value);
            row_list.add(obj_key < obj_key2 ? obj_key2.value : obj_key.value);
            m_array->set(ins_pos_refs, row_list.get_ref());
        }
        else {
            if (suboffset > s_max_offset) {
                // These strings have the same prefix up to this point but we
                // don't want to recurse further, create a list in sorted order.
                bool row_ndx_first = value < v2;
                Array row_list(alloc);
                row_list.create(Array::type_Normal); // Throws
                row_list.add(row_ndx_first ? obj_key.value : obj_key2.value);
                row_list.add(row_ndx_first ? obj_key2.value : obj_key.value);
                m_array->set(ins_pos_refs, row_list.get_ref());
            }
            else {
                // These strings have the same prefix up to this point but they
                // are actually not equal. Extend the tree recursivly until the
                // prefix of these strings is different.
                StringIndex subindex(m_target_column, m_array->get_alloc());
                subindex.insert_with_offset(obj_key2, v2, suboffset);
                subindex.insert_with_offset(obj_key, value, suboffset);
                // Join the string of SubIndices to the current position of m_array
                m_array->set(ins_pos_refs, subindex.get_ref());
            }
        }
        return true;
    }

    // If there already is a list of matches, we see if we fit there
    // or it has to be split into a subindex
    ref_type ref = ref_type(slot_value);
    char* header = alloc.translate(ref);
    if (!Array::get_context_flag_from_header(header)) {
        IntegerColumn sub(alloc, ref); // Throws
        sub.set_parent(m_array.get(), ins_pos_refs);

        SortedListComparator slc(m_target_column);
        IntegerColumn::const_iterator it_end = sub.cend();
        IntegerColumn::const_iterator lower = std::lower_bound(sub.cbegin(), it_end, value, slc);

        bool value_exists_in_list = false;
        if (lower != it_end) {
            StringConversionBuffer buffer;
            StringData lower_value = get(ObjKey(*lower), buffer);
            if (lower_value == value) {
                value_exists_in_list = true;
            }
        }

        // If we found the value in this list, add the duplicate to the list.
        if (value_exists_in_list) {
            insert_to_existing_list_at_lower(obj_key, value, sub, lower);
        }
        else {
            if (suboffset > s_max_offset) {
                insert_to_existing_list(obj_key, value, sub);
            }
            else {
#ifdef REALM_DEBUG
                bool contains_only_duplicates = true;
                if (sub.size() > 1) {
                    ObjKey first_key = ObjKey(sub.get(0));
                    ObjKey last_key = ObjKey(sub.back());
                    StringConversionBuffer first_buffer, last_buffer;
                    StringData first_str = get(first_key, first_buffer);
                    StringData last_str = get(last_key, last_buffer);
                    // Since the list is kept in sorted order, the first and
                    // last values will be the same only if the whole list is
                    // storing duplicate values.
                    if (first_str != last_str) {
                        contains_only_duplicates = false; // LCOV_EXCL_LINE
                    }
                }
                REALM_ASSERT_DEBUG(contains_only_duplicates);
#endif
                // If the list only stores duplicates we are free to branch and
                // and create a sub index with this existing list as one of the
                // leafs, but if the list doesn't only contain duplicates we
                // must respect that we store a common key prefix up to this
                // point and insert into the existing list.
                ObjKey key_of_any_dup = ObjKey(sub.get(0));
                // The buffer is needed for when this is an integer index.
                StringConversionBuffer buffer;
                StringData v2 = get(key_of_any_dup, buffer);
                StringIndex subindex(m_target_column, m_array->get_alloc());
                subindex.insert_row_list(sub.get_ref(), suboffset, v2);
                subindex.insert_with_offset(obj_key, value, suboffset);
                m_array->set(ins_pos_refs, subindex.get_ref());
            }
        }
        return true;
    }

    // The key matches, but there is a subindex here so go down a level in the tree.
    StringIndex subindex(ref, m_array.get(), ins_pos_refs, m_target_column, alloc);
    subindex.insert_with_offset(obj_key, value, suboffset);

    return true;
}

StringData StringIndex::get(ObjKey key, StringConversionBuffer& buffer) const
{
    return m_target_column.get_index_data(key, buffer);
}

void StringIndex::clear()
{
    Array values(m_array->get_alloc());
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size() + 1);

    values.clear();
    values.ensure_minimum_width(0x7FFFFFFF); // This ensures 31 bits plus a sign bit

    size_t size = 1;
    m_array->truncate_and_destroy_children(size); // Don't touch `values` array

    m_array->set_type(Array::type_HasRefs);
}


void StringIndex::do_delete(ObjKey obj_key, StringData value, size_t offset)
{
    Allocator& alloc = m_array->get_alloc();
    Array values(alloc);
    get_child(*m_array, 0, values);
    REALM_ASSERT(m_array->size() == values.size() + 1);

    // Create 4 byte index key
    key_type key = create_key(value, offset);

    const size_t pos = values.lower_bound_int(key);
    const size_t pos_refs = pos + 1; // first entry in refs points to offsets
    REALM_ASSERT(pos != values.size());

    if (m_array->is_inner_bptree_node()) {
        ref_type ref = m_array->get_as_ref(pos_refs);
        StringIndex node(ref, m_array.get(), pos_refs, m_target_column, alloc);
        node.do_delete(obj_key, value, offset);

        // Update the ref
        if (node.is_empty()) {
            values.erase(pos);
            m_array->erase(pos_refs);
            node.destroy();
        }
        else {
            key_type max_val = node.get_last_key();
            if (max_val != key_type(values.get(pos)))
                values.set(pos, max_val);
        }
    }
    else {
        uint64_t ref = m_array->get(pos_refs);
        if (ref & 1) {
            REALM_ASSERT(int64_t(ref >> 1) == obj_key.value);
            values.erase(pos);
            m_array->erase(pos_refs);
        }
        else {
            // A real ref either points to a list or a subindex
            char* header = alloc.translate(ref_type(ref));
            if (Array::get_context_flag_from_header(header)) {
                StringIndex subindex(ref_type(ref), m_array.get(), pos_refs, m_target_column, alloc);
                subindex.do_delete(obj_key, value, offset + s_index_key_length);

                if (subindex.is_empty()) {
                    values.erase(pos);
                    m_array->erase(pos_refs);
                    subindex.destroy();
                }
            }
            else {
                IntegerColumn sub(alloc, ref_type(ref)); // Throws
                sub.set_parent(m_array.get(), pos_refs);
                size_t r = sub.find_first(obj_key.value);
                size_t sub_size = sub.size(); // Slow
                REALM_ASSERT_EX(r != sub_size, r, sub_size);
                sub.erase(r);

                if (sub_size == 1) {
                    values.erase(pos);
                    m_array->erase(pos_refs);
                    sub.destroy();
                }
            }
        }
    }
}

void StringIndex::erase(ObjKey key)
{
    StringConversionBuffer buffer;
    StringData value = get(key, buffer);

    do_delete(key, value, 0);

    // Collapse top nodes with single item
    while (m_array->is_inner_bptree_node()) {
        REALM_ASSERT(m_array->size() > 1); // node cannot be empty
        if (m_array->size() > 2)
            break;

        ref_type ref = m_array->get_as_ref(1);
        m_array->set(1, 1); // avoid destruction of the extracted ref
        m_array->destroy_deep();
        m_array->init_from_ref(ref);
        m_array->update_parent();
    }
}

namespace {

bool has_duplicate_values(const Array& node, const ClusterColumn& target_col) noexcept
{
    Allocator& alloc = node.get_alloc();
    Array child(alloc);
    size_t n = node.size();
    REALM_ASSERT(n >= 1);
    if (node.is_inner_bptree_node()) {
        // Inner node
        for (size_t i = 1; i < n; ++i) {
            ref_type ref = node.get_as_ref(i);
            child.init_from_ref(ref);
            if (has_duplicate_values(child, target_col))
                return true;
        }
        return false;
    }

    // Leaf node
    for (size_t i = 1; i < n; ++i) {
        int_fast64_t value = node.get(i);
        bool is_single_row_index = (value & 1) != 0;
        if (is_single_row_index)
            continue;

        ref_type ref = to_ref(value);
        child.init_from_ref(ref);

        bool is_subindex = child.get_context_flag();
        if (is_subindex) {
            if (has_duplicate_values(child, target_col))
                return true;
            continue;
        }

        // Child is root of B+-tree of row indexes
        IntegerColumn sub(alloc, ref);
        if (sub.size() > 1) {
            ObjKey first_key = ObjKey(sub.get(0));
            ObjKey last_key = ObjKey(sub.back());
            StringConversionBuffer first_buffer, last_buffer;
            StringData first_str = target_col.get_index_data(first_key, first_buffer);
            StringData last_str = target_col.get_index_data(last_key, last_buffer);
            // Since the list is kept in sorted order, the first and
            // last values will be the same only if the whole list is
            // storing duplicate values.
            if (first_str == last_str) {
                return true;
            }
            // There may also be several short lists combined, so we need to
            // check each of these individually for duplicates.
            IntegerColumn::const_iterator it = sub.cbegin();
            IntegerColumn::const_iterator it_end = sub.cend();
            SortedListComparator slc(target_col);
            StringConversionBuffer buffer;
            while (it != it_end) {
                StringData it_data = target_col.get_index_data(ObjKey(*it), buffer);
                IntegerColumn::const_iterator next = std::upper_bound(it, it_end, it_data, slc);
                size_t count_of_value = next - it; // row index subtraction in `sub`
                if (count_of_value > 1) {
                    return true;
                }
                it = next;
            }
        }
    }

    return false;
}

} // anonymous namespace


bool StringIndex::has_duplicate_values() const noexcept
{
    return ::has_duplicate_values(*m_array, m_target_column);
}


bool StringIndex::is_empty() const
{
    return m_array->size() == 1; // first entry in refs points to offsets
}


void StringIndex::node_add_key(ref_type ref)
{
    REALM_ASSERT(ref);
    REALM_ASSERT(m_array->is_inner_bptree_node());

    Allocator& alloc = m_array->get_alloc();
    Array offsets(alloc);
    get_child(*m_array, 0, offsets);
    REALM_ASSERT(m_array->size() == offsets.size() + 1);
    REALM_ASSERT(offsets.size() < REALM_MAX_BPNODE_SIZE + 1);

    Array new_top(alloc);
    Array new_offsets(alloc);
    new_top.init_from_ref(ref);
    new_offsets.init_from_ref(new_top.get_as_ref(0));
    REALM_ASSERT(!new_offsets.is_empty());

    int64_t key = new_offsets.back();
    offsets.add(key);
    m_array->add(ref);
}

// Must return true if value of object(key) is less than needle.
bool SortedListComparator::operator()(int64_t key_value, StringData needle) // used in lower_bound
{
    // The buffer is needed when for when this is an integer index.
    StringConversionBuffer buffer;
    StringData a = m_column.get_index_data(ObjKey(key_value), buffer);
    if (a.is_null() && !needle.is_null())
        return true;
    else if (needle.is_null() && !a.is_null())
        return false;
    else if (a.is_null() && needle.is_null())
        return false;

    if (a == needle)
        return false;

    // The StringData::operator< uses a lexicograpical comparison, therefore we
    // cannot use our utf8_compare here because we need to be consistent with
    // using the same compare method as how these strings were they were put
    // into this ordered column in the first place.
    return a < needle;
}


// Must return true if value of needle is less than value of object(key).
bool SortedListComparator::operator()(StringData needle, int64_t key_value) // used in upper_bound
{
    StringConversionBuffer buffer;
    StringData a = m_column.get_index_data(ObjKey(key_value), buffer);
    if (needle == a) {
        return false;
    }
    return !(*this)(key_value, needle);
}

// LCOV_EXCL_START ignore debug functions


void StringIndex::verify() const
{
#ifdef REALM_DEBUG
    m_array->verify();

    Allocator& alloc = m_array->get_alloc();
    const size_t array_size = m_array->size();

    // Get first matching row for every key
    if (m_array->is_inner_bptree_node()) {
        for (size_t i = 1; i < array_size; ++i) {
            size_t ref = m_array->get_as_ref(i);
            StringIndex ndx(ref, nullptr, 0, m_target_column, alloc);
            ndx.verify();
        }
    }
    else {
        size_t column_size = m_target_column.size();
        for (size_t i = 1; i < array_size; ++i) {
            int64_t ref = m_array->get(i);

            // low bit set indicate literal ref (shifted)
            if (ref & 1) {
                size_t r = to_size_t((uint64_t(ref) >> 1));
                REALM_ASSERT_EX(r < column_size, r, column_size);
            }
            else {
                // A real ref either points to a list or a subindex
                char* header = alloc.translate(to_ref(ref));
                if (Array::get_context_flag_from_header(header)) {
                    StringIndex ndx(to_ref(ref), m_array.get(), i, m_target_column, alloc);
                    ndx.verify();
                }
                else {
                    IntegerColumn sub(alloc, to_ref(ref)); // Throws
                    IntegerColumn::const_iterator it = sub.cbegin();
                    IntegerColumn::const_iterator it_end = sub.cend();
                    SortedListComparator slc(m_target_column);
                    StringConversionBuffer buffer, buffer_prev;
                    StringData previous_string = get(ObjKey(*it), buffer_prev);
                    size_t last_row = to_size_t(*it);

                    // Check that strings listed in sub are in sorted order
                    // and if there are duplicates, that the row numbers are
                    // sorted in the group of duplicates.
                    while (it != it_end) {
                        StringData it_data = get(ObjKey(*it), buffer);
                        size_t it_row = to_size_t(*it);
                        REALM_ASSERT_EX(previous_string <= it_data, previous_string.data(), it_data.data());
                        if (it != sub.cbegin() && previous_string == it_data) {
                            REALM_ASSERT_EX(it_row > last_row, it_row, last_row);
                        }
                        last_row = it_row;
                        previous_string = get(ObjKey(*it), buffer_prev);
                        ++it;
                    }
                }
            }
        }
    }
// FIXME: Extend verification along the lines of IntegerColumn::verify().
#endif
}

#ifdef REALM_DEBUG

template <class T>
void StringIndex::verify_entries(const ClusterColumn& column) const
{
    std::vector<ObjKey> results;

    auto it = column.begin();
    auto end = column.end();
    auto col = column.get_column_key();
    while (it != end) {
        ObjKey key = it->get_key();
        T value = it->get<T>(col);

        find_all(results, value);

        auto ndx = find(results.begin(), results.end(), key);
        REALM_ASSERT(ndx != results.end());
        size_t found = count(value);
        REALM_ASSERT_EX(found >= 1, found);
        results.clear();
    }
}

void StringIndex::dump_node_structure(const Array& node, std::ostream& out, int level)
{
    int indent = level * 2;
    Allocator& alloc = node.get_alloc();
    Array subnode(alloc);

    size_t node_size = node.size();
    REALM_ASSERT(node_size >= 1);

    bool node_is_leaf = !node.is_inner_bptree_node();
    if (node_is_leaf) {
        out << std::setw(indent) << ""
            << "Leaf (B+ tree) (ref: " << node.get_ref() << ")\n";
    }
    else {
        out << std::setw(indent) << ""
            << "Inner node (B+ tree) (ref: " << node.get_ref() << ")\n";
    }

    subnode.init_from_ref(to_ref(node.front()));
    out << std::setw(indent) << ""
        << "  Keys (keys_ref: " << subnode.get_ref() << ", ";
    if (subnode.is_empty()) {
        out << "no keys";
    }
    else {
        out << "keys: ";
        for (size_t i = 0; i != subnode.size(); ++i) {
            if (i != 0)
                out << ", ";
            out << subnode.get(i);
        }
    }
    out << ")\n";

    if (node_is_leaf) {
        for (size_t i = 1; i != node_size; ++i) {
            int_fast64_t value = node.get(i);
            bool is_single_row_index = (value & 1) != 0;
            if (is_single_row_index) {
                out << std::setw(indent) << ""
                    << "  Single row index (value: " << (value / 2) << ")\n";
                continue;
            }
            subnode.init_from_ref(to_ref(value));
            bool is_subindex = subnode.get_context_flag();
            if (is_subindex) {
                out << std::setw(indent) << ""
                    << "  Subindex\n";
                dump_node_structure(subnode, out, level + 2);
                continue;
            }
            IntegerColumn indexes(alloc, to_ref(value));
            out << std::setw(indent) << ""
                << "  List of row indexes\n";
            indexes.dump_values(out, level + 2);
        }
        return;
    }


    size_t num_children = node_size - 1;
    size_t child_ref_begin = 1;
    size_t child_ref_end = 1 + num_children;
    for (size_t i = child_ref_begin; i != child_ref_end; ++i) {
        subnode.init_from_ref(node.get_as_ref(i));
        dump_node_structure(subnode, out, level + 1);
    }
}


void StringIndex::do_dump_node_structure(std::ostream& out, int level) const
{
    dump_node_structure(*m_array, out, level);
}

#endif // LCOV_EXCL_STOP ignore debug functions
