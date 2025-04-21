package org.springframework.data.aerospike.repository.query.blocking.find;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

/**
 * Tests for the "Is false" repository query. Keywords: False, IsFalse.
 */
public class FalseTests extends PersonRepositoryQueryTests {

    @Test
    void findByBooleanSimplePropertyIsFalse() {
        Person intBoolBinPerson = Person.builder().id(nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        Assertions.assertThat(repository.findByIsActiveFalse()).doesNotContain(intBoolBinPerson);
        repository.delete(intBoolBinPerson);
    }
}
