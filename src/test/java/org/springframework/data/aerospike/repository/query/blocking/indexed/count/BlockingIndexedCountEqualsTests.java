package org.springframework.data.aerospike.repository.query.blocking.indexed.count;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Nightly;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
@Nightly
public class BlockingIndexedCountEqualsTests extends IndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        newIndexes.add(Index.builder()
                .set(template.getSetName(IndexedPerson.class))
                .name("indexed_person_last_name_" + "count_equals")
                .bin("lastName")
                .indexType(STRING)
                .build());
        return newIndexes;
    }

    @Test
    @AssertBinsAreIndexed(binNames = "lastName", entityClass = IndexedPerson.class)
    public void countBySimpleProperty_String() {
        assertThat(repository.countByLastName("Lerois")).isZero();
        assertThat(repository.countByLastName("James")).isEqualTo(1);

        assertQueryHasSecIndexFilter("countByLastName", IndexedPerson.class, "James");
        assertThat(repository.countByLastName("James")).isEqualTo(1);
    }
}
