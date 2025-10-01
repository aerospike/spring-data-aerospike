package org.springframework.data.aerospike.repository.query.blocking.indexed.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexCollectionType.MAPKEYS;
import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.VALUE;

/**
 * Tests for the "Does not contain" repository query. Keywords: IsNotContaining, NotContaining, NotContains.
 */
@Extensive
public class BlockingIndexedFindNotContainingTests extends IndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = template.getSetName(IndexedPerson.class);
        String postfix = "find_not_containing";
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
        return newIndexes;
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    void findByMapKeysNotContaining_String_NoSecondaryIndexFilter() {
        assertThat(billy.getStringMap()).containsKey("key1");
        // "Not containing" has no secondary index Filter
        assertThat(queryHasSecIndexFilter("findByStringMapNotContaining", IndexedPerson.class, KEY, "key3"))
                .isFalse();

        List<IndexedPerson> persons = repository.findByStringMapNotContaining(KEY, "key3");
        assertThat(persons).contains(billy);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "stringMap", entityClass = IndexedPerson.class)
    void findByMapValuesNotContaining_String() {
        assertThat(billy.getStringMap()).containsValue("val1");
        // "Not containing" has no secondary index Filter
        assertThat(queryHasSecIndexFilter("findByStringMapNotContaining", IndexedPerson.class, VALUE, "val3"))
                .isFalse();

        List<IndexedPerson> persons = repository.findByStringMapNotContaining(VALUE, "val3");
        assertThat(persons).contains(billy);
    }
}
