package org.springframework.data.aerospike.repository.query.findBy.noindex;

import com.aerospike.client.Value;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.Person;

/**
 * Tests for the "Is false" repository query. Keywords: False, IsFalse.
 */
public class IsFalseTests extends PersonRepositoryQueryTests {

    @Test
    void findByBooleanIntSimplePropertyIsFalse() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = false; // save boolean as int
        Person intBoolBinPerson = Person.builder().id(BaseIntegrationTests.nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        Assertions.assertThat(repository.findByIsActiveFalse()).doesNotContain(intBoolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findByBooleanSimplePropertyIsFalse() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool, available in Server 5.6+
        Person intBoolBinPerson = Person.builder().id(BaseIntegrationTests.nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        Assertions.assertThat(repository.findByIsActiveFalse()).doesNotContain(intBoolBinPerson);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }
}
