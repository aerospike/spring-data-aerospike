package org.springframework.data.aerospike.repository.query.blocking.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is true" repository query. Keywords: True, IsTrue.
 */
public class TrueTests extends PersonRepositoryQueryTests {

    @Test
    void findByBooleanSimplePropertyIsTrue() {
        Person intBoolBinPerson = Person.builder().id(nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons1 = repository.findByIsActiveTrue();
        assertThat(persons1).contains(intBoolBinPerson);

        List<Person> persons2 = repository.findByIsActiveIsTrue(); // another way to call the query method
        assertThat(persons2).containsExactlyElementsOf(persons1);
        repository.delete(intBoolBinPerson);
    }
}
