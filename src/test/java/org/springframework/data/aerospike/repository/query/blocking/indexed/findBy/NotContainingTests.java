package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.VALUE;

/**
 * Tests for the "Does not contain" repository query. Keywords: IsNotContaining, NotContaining, NotContains.
 */
public class NotContainingTests extends IndexedPersonRepositoryQueryTests {

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    void findByMapKeysNotContaining_String_NoSecondaryIndexFilter() {
        assertThat(billy.getStringMap()).containsKey("key1");
        // "Not containing" has no secondary index Filter
        assertThat(stmtHasSecIndexFilter("findByStringMapNotContaining", IndexedPerson.class, KEY, "key3")).isFalse();

        List<IndexedPerson> persons = repository.findByStringMapNotContaining(KEY, "key3");
        assertThat(persons).contains(billy);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    void findByMapValuesNotContaining_String() {
        assertThat(billy.getStringMap()).containsValue("val1");
        // "Not containing" has no secondary index Filter
        assertThat(stmtHasSecIndexFilter("findByStringMapNotContaining", IndexedPerson.class, VALUE, "val3")).isFalse();

        List<IndexedPerson> persons = repository.findByStringMapNotContaining(VALUE, "val3");
        assertThat(persons).contains(billy);
    }
}
