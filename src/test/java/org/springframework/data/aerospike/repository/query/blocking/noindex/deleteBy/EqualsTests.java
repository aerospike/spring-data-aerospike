package org.springframework.data.aerospike.repository.query.blocking.noindex.deleteBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends PersonRepositoryQueryTests {

    @Test
    void deleteBySimplePropertyEquals_String() {
        assertThat(repository.findByFirstName("Leroi")).isNotEmpty();
        repository.deleteByFirstName("Leroi");
        assertThat(repository.findByFirstName("Leroi")).isEmpty();

        repository.save(leroi);
        assertThat(repository.findByFirstNameIgnoreCase("lEroi")).isNotEmpty();
        repository.deleteByFirstNameIgnoreCase("lEroi");
        assertThat(repository.findByFirstNameIgnoreCase("lEroi")).isEmpty();
    }

    @Test
    void deletePersonById_AND_simpleProperty() {
//        QueryParam ids = of(dave.getId());
//        QueryParam name = of(carter.getFirstName());
//        repository.deleteByIdAndFirstName(ids, name);
//        assertThat(repository.findByIdAndFirstName(ids, name)).isEmpty();
//
//        ids = of(dave.getId());
//        name = of(dave.getFirstName());
//        assertThat(repository.findByIdAndFirstName(ids, name)).isNotEmpty();
//        repository.deleteByIdAndFirstName(ids, name);
//        assertThat(repository.findByIdAndFirstName(ids, name)).isEmpty();

        repository.deleteAllById(List.of(dave.getId(), carter.getId()));
    }
}
