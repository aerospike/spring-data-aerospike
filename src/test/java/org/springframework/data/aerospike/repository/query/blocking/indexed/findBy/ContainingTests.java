package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY_VALUE_PAIR;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.VALUE;

/**
 * Tests for the "Contains" repository query. Keywords: Containing, IsContaining, Contains.
 */
public class ContainingTests extends IndexedPersonRepositoryQueryTests {

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
}
