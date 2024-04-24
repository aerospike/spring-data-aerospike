package org.springframework.data.aerospike.repository.query.blocking.indexed.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.config.NoSecondaryIndexRequired;
import org.springframework.data.aerospike.repository.query.blocking.indexed.IndexedPersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.IndexedPerson;
import org.springframework.data.aerospike.sample.Person;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the CrudRepository queries API.
 */
public class CrudRepositoryQueryTests extends IndexedPersonRepositoryQueryTests {

    @Test
    @NoSecondaryIndexRequired
    public void findsPersonById() {
        Optional<IndexedPerson> person = repository.findById(john.getId());

        assertThat(person).hasValueSatisfying(actual -> {
            assertThat(actual).isInstanceOf(Person.class);
            assertThat(actual).isEqualTo(john);
        });
    }

    @Test
    @NoSecondaryIndexRequired
    public void findsAllWithGivenIds() {
        List<IndexedPerson> result = (List<IndexedPerson>) repository.findAllById(List.of(john.getId(),
            billy.getId()));

        assertThat(result)
            .contains(john, billy)
            .hasSize(2)
            .doesNotContain(jane, peter, tricia);
    }
}
