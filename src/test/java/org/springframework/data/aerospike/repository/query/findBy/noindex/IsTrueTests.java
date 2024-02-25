package org.springframework.data.aerospike.repository.query.findBy.noindex;

import com.aerospike.client.Value;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Tests for the "Is true" repository query. Keywords: True, IsTrue.
 */
public class IsTrueTests extends PersonRepositoryQueryTests {

    @Test
    void findByBooleanIntSimplePropertyIsTrue() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = false; // save boolean as int
        Person intBoolBinPerson = Person.builder().id(BaseIntegrationTests.nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons1 = repository.findByIsActiveTrue();
        assertThat(persons1).contains(intBoolBinPerson);

        List<Person> persons2 = repository.findByIsActiveIsTrue(); // another way to call the query method
        assertThat(persons2).containsExactlyElementsOf(persons1);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }

    @Test
    void findByBooleanSimplePropertyIsTrue() {
        boolean initialValue = Value.UseBoolBin;
        Value.UseBoolBin = true; // save boolean as bool, available in Server 5.6+
        Person intBoolBinPerson = Person.builder().id(BaseIntegrationTests.nextId()).isActive(true).firstName("Test")
            .build();
        repository.save(intBoolBinPerson);

        List<Person> persons1 = repository.findByIsActiveTrue();
        assertThat(persons1).contains(intBoolBinPerson);

        List<Person> persons2 = repository.findByIsActiveIsTrue(); // another way to call the query method
        assertThat(persons2).containsExactlyElementsOf(persons1);

        Value.UseBoolBin = initialValue; // set back to the default value
        repository.delete(intBoolBinPerson);
    }
}
