package org.springframework.data.aerospike.repository.query.reactive.indexed.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.repository.query.reactive.indexed.ReactiveIndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;

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
public class ReactiveIndexedFindContainingTests extends ReactiveIndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = reactiveTemplate.getSetName(IndexedPerson.class);
        String postfix = "r_find_contains";
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
    public void findByCollectionContaining_String() {
        assertQueryHasSecIndexFilter("findByStringsContaining", IndexedPerson.class, "str1");
        List<IndexedPerson> results = reactiveRepository.findByStringsContaining("str1")
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsExactlyInAnyOrder(alain);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "ints", entityClass = IndexedPerson.class)
    public void findByCollectionContaining_Integer() {
        assertQueryHasSecIndexFilter("findByIntsContaining", IndexedPerson.class, 550);
        List<IndexedPerson> results = reactiveRepository.findByIntsContaining(550)
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsExactlyInAnyOrder(daniel, emilien);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    public void findByMapKeysContaining_String() {
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, KEY, "key1");
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining(KEY, "key1")
                .subscribeOn(Schedulers.parallel()).collectList().block();

        assertThat(results).contains(luc, petra);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    public void findByMapValuesContaining_String() {
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, VALUE, "val1");
        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining(VALUE, "val1")
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).contains(luc, petra);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    public void findByExactMapKeyAndValue_String() {
        assertThat(petra.getStringMap().containsKey("key1")).isTrue();
        assertThat(petra.getStringMap().containsValue("val1")).isTrue();
        assertThat(luc.getStringMap().containsKey("key1")).isTrue();
        assertThat(luc.getStringMap().containsValue("val1")).isTrue();
        assertQueryHasSecIndexFilter("findByStringMapContaining", IndexedPerson.class, KEY_VALUE_PAIR, "key1", "val1");

        List<IndexedPerson> results = reactiveRepository.findByStringMapContaining(KEY_VALUE_PAIR, "key1", "val1")
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).contains(petra, luc);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "intMap", entityClass = IndexedPerson.class)
    public void findByExactMapKeyAndValue_Integer() {
        assertThat(emilien.getIntMap().containsKey("key1")).isTrue();
        assertThat(emilien.getIntMap().get("key1")).isZero();
        assertThat(lilly.getIntMap().containsKey("key1")).isTrue();
        assertThat(lilly.getIntMap().get("key1")).isNotZero();
        assertQueryHasSecIndexFilter("findByIntMapContaining", IndexedPerson.class, KEY_VALUE_PAIR, "key1", 0);

        List<IndexedPerson> results = reactiveRepository.findByIntMapContaining(KEY_VALUE_PAIR, "key1", 0)
                .subscribeOn(Schedulers.parallel()).collectList().block();
        assertThat(results).containsExactly(emilien);
    }
}
