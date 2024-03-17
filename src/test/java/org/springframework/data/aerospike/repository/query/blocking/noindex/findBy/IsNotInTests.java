package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is not in" repository query. Keywords: NotIn, IsNotIn.
 */
public class IsNotInTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyNotIn_String() {
        Collection<String> firstNames;
        firstNames = allPersons.stream().map(Person::getFirstName).collect(Collectors.toSet());
        assertThat(repository.findByFirstNameNotIn(firstNames)).isEmpty();

        firstNames = List.of("Dave", "Donny", "Carter", "Boyd", "Leroi", "Stefan", "Matias", "Douglas");
        assertThat(repository.findByFirstNameNotIn(firstNames)).containsExactlyInAnyOrder(oliver, alicia);
    }
}
