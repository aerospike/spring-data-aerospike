package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.Date;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is after" repository query. Keywords: After, IsAfter.
 */
public class IsAfterTests extends PersonRepositoryQueryTests {

    @Test
    void findByDateSimplePropertyAfter() {
        dave.setDateOfBirth(new Date());
        repository.save(dave);

        List<Person> persons = repository.findByDateOfBirthAfter(new Date(126230400));
        assertThat(persons).contains(dave);

        dave.setDateOfBirth(null);
        repository.save(dave);
    }
}
