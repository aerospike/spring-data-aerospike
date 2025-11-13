package org.springframework.data.aerospike.repository.query.blocking.indexed.find;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aerospike.client.query.IndexCollectionType.LIST;
import static com.aerospike.client.query.IndexCollectionType.MAPKEYS;
import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY_VALUE_PAIR;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.VALUE;

/**
 * Tests for the "Contains" repository query. Keywords: Containing, IsContaining, Contains.
 */
@Extensive
public class BlockingIndexedFindContainingTests extends IndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = template.getSetName(IndexedPerson.class);
        String postfix = "find_contains";
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_strings_" + postfix)
                .bin("strings")
                .indexType(STRING)
                .indexCollectionType(LIST)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_ints_" + postfix)
                .bin("ints")
                .indexType(NUMERIC)
                .indexCollectionType(LIST)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_string_map_keys_" + postfix)
                .bin("stringMap")
                .indexType(STRING)
                .indexCollectionType(MAPKEYS)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_string_map_values_" + postfix)
                .bin("stringMap")
                .indexType(STRING)
                .indexCollectionType(MAPVALUES).build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_int_map_keys_" + postfix)
                .bin("intMap")
                .indexType(STRING)
                .indexCollectionType(MAPKEYS)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_int_map_values_" + postfix)
                .bin("intMap")
                .indexType(NUMERIC)
                .indexCollectionType(MAPVALUES)
                .build());
        return newIndexes;
    }

    @Test
    @AssertBinsAreIndexed(binNames = "strings", entityClass = IndexedPerson.class)
    void findByCollectionContaining_String() {

        assertQueryHasSecIndexFilter("findByStringsContaining", IndexedPerson.class, "str1");
        assertThat(repository.findByStringsContaining("str1")).containsOnly(john, peter);
        assertThat(repository.findByStringsContaining("str2")).containsOnly(john, peter);
        assertThat(repository.findByStringsContaining("str3")).containsOnly(peter);
        assertThat(repository.findByStringsContaining("str5")).isEmpty();
    }

    @Test
    @AssertBinsAreIndexed(binNames = "ints", entityClass = IndexedPerson.class)
    void findByCollectionContaining_Integer() {
        assertQueryHasSecIndexFilter("findByIntsContaining", IndexedPerson.class, 550);
        assertThat(repository.findByIntsContaining(550)).containsOnly(john, jane);
        assertThat(repository.findByIntsContaining(990)).containsOnly(john, jane);
        assertThat(repository.findByIntsContaining(600)).containsOnly(jane);
        assertThat(repository.findByIntsContaining(7777)).isEmpty();
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    void findByMapKeysContaining_String() {
        assertThat(billy.getStringMap()).containsKey("key1");
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, KEY, "key1");

        List<IndexedPerson> persons = repository.findByStringMapContaining(KEY, "key1");
        assertThat(persons).contains(billy);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    void findByMapValuesContaining_String() {
        assertThat(billy.getStringMap()).containsValue("val1");
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, VALUE, "key1");

        List<IndexedPerson> persons = repository.findByStringMapContaining(VALUE, "val1");
        assertThat(persons).contains(billy);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "intMap", entityClass = IndexedPerson.class)
    void findByExactMapKeyAndValue_Integer() {
        assertThat(tricia.getIntMap()).containsKey("key1");
        assertThat(tricia.getIntMap().get("key1")).isEqualTo(0);
        assertQueryHasSecIndexFilter("findByIntMapContaining", IndexedPerson.class, KEY_VALUE_PAIR, "key1", 0);

        Iterable<IndexedPerson> result = repository.findByIntMapContaining(KEY_VALUE_PAIR, "key1", 0);
        assertThat(result).contains(tricia);
    }

    @Test
    void findByNestedCollectionContainingString_NoIndexForList() {
        additionalAerospikeTestOperations.createIndex(IndexedPerson.class, "person_listOfIntLists_values_index",
                "listOfIntLists", IndexType.NUMERIC, LIST, CTX.listValue(Value.get(List.of(100))));

        if (serverVersionSupport.isFindByCDTSupported()) {
            List<List<Integer>> listOfLists1 = List.of(List.of(100));
            john.setListOfIntLists(listOfLists1);
            repository.save(john);
            List<Integer> list = new ArrayList<>();
            list.add(100);
            // there is no sIndex filter for a List
            assertQueryHasNoSecIndexFilter("findByListOfIntListsContaining", IndexedPerson.class, list);

            List<IndexedPerson> persons = repository.findByListOfIntListsContaining(List.of(100));
            assertThat(persons).contains(john);
        }
        additionalAerospikeTestOperations.dropIndex(IndexedPerson.class, "person_listOfIntLists_values_index");
    }

    @Test
    void findByNestedMapValuesContainingString() {
        additionalAerospikeTestOperations.createIndex(IndexedPerson.class, "person_friend_stringMap_values_index",
                "friend", IndexType.STRING, IndexCollectionType.MAPVALUES, CTX.mapKey(Value.get("stringMap")));

        billy.setStringMap(Map.of("key1", "val1"));
        repository.save(billy);
        tricia.setStringMap(Map.of("key1", "val1"));
        repository.save(tricia);

        peter.setFriend(billy);
        repository.save(peter);
        jane.setFriend(tricia);
        repository.save(jane);

        List<IndexedPerson> persons = repository.findByFriendStringMapContaining(VALUE, "val1");
        assertThat(persons).contains(peter, jane);
        TestUtils.setFriendsToNull(repository, peter, jane);
    }
}
