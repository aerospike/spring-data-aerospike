package org.springframework.data.aerospike.repository.query.blocking.noindex.deleteBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

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
        // cleanup
        repository.save(leroi);
    }

    @Test
    void deleteById() {
        assertThat(repository.existsById(dave.getId())).isTrue();
        repository.deleteById(dave.getId());
        assertThat(repository.existsById(dave.getId())).isFalse();
        // cleanup
        repository.save(dave);
    }

    @Test
    void deleteById_AND_simpleProperty() {
        assertThat(repository.existsById(leroi.getId())).isTrue();
        assertThat(repository.existsById(dave.getId())).isTrue();
        assertThat(repository.existsById(carter.getId())).isTrue();

        QueryParam id = of(leroi.getId());
        QueryParam name = of(leroi.getFirstName());
        assertThat(repository.existsByIdAndFirstName(id, name)).isTrue();
        repository.deleteByIdAndFirstName(id, name);
        assertThat(repository.existsByIdAndFirstName(id, name)).isFalse();

        QueryParam id2 = of(dave.getId());
        QueryParam name2 = of(carter.getFirstName());
        // there is no record with Dave's id and Carter's first name
        assertThat(repository.existsByIdAndFirstName(id2, name2)).isFalse();
        // delete using id and first name of different records must not change anything
        repository.deleteByIdAndFirstName(id2, name2);
        assertThat(repository.existsById(dave.getId())).isTrue();
        assertThat(repository.existsById(carter.getId())).isTrue();

        repository.deleteAllById(List.of(dave.getId(), carter.getId()));
        assertThat(repository.existsById(dave.getId())).isFalse();
        assertThat(repository.existsById(carter.getId())).isFalse();

        // cleanup
        repository.save(leroi);
        repository.save(dave);
        repository.save(carter);
    }

    @Test
    void deleteAllByIds() {
        assertThat(repository.existsById(dave.getId())).isTrue();
        assertThat(repository.existsById(carter.getId())).isTrue();

        repository.deleteAllById(List.of(dave.getId(), carter.getId()));

        assertThat(repository.existsById(dave.getId())).isFalse();
        assertThat(repository.existsById(carter.getId())).isFalse();

        // cleanup
        repository.save(dave);
        repository.save(carter);
    }

    @Test
    void deleteAll() {
        if (serverVersionSupport.isBatchWriteSupported()) {
            // batch delete requires server ver. >= 6.0.0
            repository.deleteAll(List.of(dave, carter));
        } else {
            List.of(dave, carter).forEach(repository::delete);
        }
        assertThat(repository.findAllById(List.of(dave.getId(), carter.getId()))).isEmpty();
        // cleanup
        repository.save(dave);
        repository.save(carter);
    }
}
