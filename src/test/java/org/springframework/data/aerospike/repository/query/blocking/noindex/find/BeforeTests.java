package org.springframework.data.aerospike.repository.query.blocking.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is before" repository query. Keywords: Before, IsBefore.
 */
public class BeforeTests extends PersonRepositoryQueryTests {

    @Test
    void findByLocalDateSimplePropertyBefore() {
        dave.setRegDate(LocalDate.of(1980, 3, 10));
        repository.save(dave);

        List<Person> persons = repository.findByRegDateBefore(LocalDate.of(1981, 3, 10));
        assertThat(persons).contains(dave);

        dave.setDateOfBirth(null);
        repository.save(dave);
    }
}
