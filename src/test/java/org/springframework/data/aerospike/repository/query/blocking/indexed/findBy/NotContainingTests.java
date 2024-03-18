package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
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
    void findByMapKeysNotContaining_String() {
        assertThat(billy.getStringMap()).containsKey("key1");

        List<IndexedPerson> persons = repository.findByStringMapNotContaining(KEY, "key3");
        assertThat(persons).contains(billy);
    }

    @Test
    void findByMapValuesNotContaining_String() {
        assertThat(billy.getStringMap()).containsValue("val1");

        List<IndexedPerson> persons = repository.findByStringMapNotContaining(VALUE, "val3");
        assertThat(persons).contains(billy);
    }
}
