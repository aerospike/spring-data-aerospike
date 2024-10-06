package org.springframework.data.aerospike.repository.query.blocking.find;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the CrudRepository queries API.
 */
public class CrudRepositoryQueryTests extends PersonRepositoryQueryTests {

    @Test
    void findById() {
        Optional<Person> person = repository.findById(dave.getId());

        assertThat(person).hasValueSatisfying(actual -> {
            assertThat(actual).isInstanceOf(Person.class);
            assertThat(actual).isEqualTo(dave);
        });
    }

    @Test
    void findAllByIds() {
        Iterable<Person> result = repository.findAllById(List.of(dave.getId(), carter.getId()));
        assertThat(result).containsExactlyInAnyOrder(dave, carter);
    }

    @Test
    void findAll() {
        List<Person> result = (List<Person>) repository.findAll();
        assertThat(result).containsExactlyInAnyOrderElementsOf(allPersons);
    }

    @Test
    void findAll_Paginated() {
        Page<Person> result = repository.findAll(PageRequest.of(1, 2, Sort.Direction.ASC, "lastname", "firstname"));
        assertThat(result.isFirst()).isFalse();
        assertThat(result.isLast()).isFalse();
    }

    @Test
    void findAll_doesNotFindDeletedPersonByEntity() {
        try {
            repository.delete(dave);
            List<Person> result = (List<Person>) repository.findAll();
            assertThat(result)
                .doesNotContain(dave)
                .containsExactlyInAnyOrderElementsOf(
                    allPersons.stream().filter(person -> !person.equals(dave)).collect(Collectors.toList())
                );
        } finally {
            repository.save(dave);
        }
    }

    @Test
    void findAll_doesNotFindPersonDeletedById() {
        try {
            repository.deleteById(dave.getId());
            List<Person> result = (List<Person>) repository.findAll();
            assertThat(result)
                .doesNotContain(dave)
                .hasSize(allPersons.size() - 1);
        } finally {
            repository.save(dave);
        }
    }
}
