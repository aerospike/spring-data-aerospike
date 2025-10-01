package org.springframework.data.aerospike.repository.query.blocking.indexed.count;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.config.AssertBinsAreIndexed;
import org.springframework.data.aerospike.config.NoSecondaryIndexRequired;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is between" repository query. Keywords: Between, IsBetween.
 */
@Extensive
public class BlockingIndexedCountBetweenTests extends IndexedPersonRepositoryQueryTests {

    @Override
    protected List<Index> newIndexes() {
        List<Index> newIndexes = new ArrayList<>();
        String setName = template.getSetName(IndexedPerson.class);
        String postfix = "count_between";
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_first_name_" + postfix)
                .bin("firstName")
                .indexType(STRING)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_age_" + postfix)
                .bin("age")
                .indexType(NUMERIC)
                .build());
        newIndexes.add(Index.builder()
                .set(setName)
                .name("indexed_person_address_values_num_" + postfix)
                .bin("address")
                .indexType(NUMERIC)
                .indexCollectionType(MAPVALUES)
                .build());
        return newIndexes;
    }

    @Test
    @AssertBinsAreIndexed(binNames = "age", entityClass = IndexedPerson.class)
    void countBySimplePropertyBetween_Integer() {
        assertThat(john.getAge()).isBetween(40, 46);
        assertThat(peter.getAge()).isBetween(40, 46);
        assertQueryHasSecIndexFilter("countByAgeBetween", IndexedPerson.class, 40, 46);

        long result = repository.countByAgeBetween(40, 46);
        assertThat(result).isGreaterThan(0);
    }

    @Test
    @NoSecondaryIndexRequired
    void countBySimplePropertyBetween_String() {
        assertThat(indexesCache.hasIndexFor(
                new IndexedField(getNameSpace(), template.getSetName(IndexedPerson.class), "firstName"))
        ).isTrue();
        // Currently there is no SIndex Filter analogous to "find by Strings between"
        assertQueryHasNoSecIndexFilter("countByFirstNameBetween", IndexedPerson.class, "Jane", "John");
        long result = repository.countByFirstNameBetween("Jane", "John");
        assertThat(result).isEqualTo(1);
    }

    @Test
    @AssertBinsAreIndexed(binNames = "address", entityClass = IndexedPerson.class)
    void countByNestedSimplePropertyBetween_Integer() {
        assertThat(jane.getAddress().getApartment()).isEqualTo(2);
        assertThat(john.getAddress().getApartment()).isEqualTo(1);
        assertQueryHasSecIndexFilter("countByAddressApartmentBetween", IndexedPerson.class, 1, 3);
        assertThat(repository.countByAddressApartmentBetween(1, 3)).isEqualTo(2);
    }
}
