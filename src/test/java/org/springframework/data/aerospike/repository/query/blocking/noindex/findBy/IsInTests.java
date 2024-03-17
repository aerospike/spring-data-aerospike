package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is in" repository query. Keywords: In, IsIn.
 */
public class IsInTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyIn_String() {
        Stream<Person> result;
        result = repository.findByFirstNameIn(List.of("Anastasiia", "Daniil"));
        assertThat(result).isEmpty();

        result = repository.findByFirstNameIn(List.of("Alicia", "Stefan"));
        assertThat(result).contains(alicia, stefan);
    }
}
