package org.springframework.data.aerospike.repository.query.blocking.delete;

import com.aerospike.client.AerospikeException;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends PersonRepositoryQueryTests {

    @Test
    void deleteBySimplePropertyEquals_String() {
        assertThat(repository.findByFirstName("Leroi")).isNotEmpty().hasSize(2);
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
    void deleteById_shouldIgnoreNonExistent() {
        assertThat(repository.existsById("1")).isFalse();
        repository.deleteById("1");
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
    void deleteAllById_shouldIgnoreNonExistent() {
        // Pre-deletion check
        assertThat(repository.existsById(dave.getId())).isTrue();
        assertThat(repository.existsById(carter.getId())).isTrue();
        assertThat(repository.existsById("1")).isFalse();
        assertThat(repository.existsById("2")).isFalse();

        repository.deleteAllById(List.of("1", dave.getId(), "2", carter.getId()));
        assertThat(repository.existsById(dave.getId())).isFalse();
        assertThat(repository.existsById(carter.getId())).isFalse();

        // Restore records
        repository.save(dave);
        repository.save(carter);

        // Pre-deletion check
        assertThat(repository.existsById(dave.getId())).isTrue();
        assertThat(repository.existsById(carter.getId())).isTrue();
        assertThat(repository.existsById("1")).isFalse();
        assertThat(repository.existsById("2")).isFalse();

        // Another way to run the same, this is the implementation of repository.deleteAllById(Iterable<?>)
        template.deleteByIds(List.of("1", dave.getId(), "2", carter.getId()), Person.class);
        assertThat(repository.existsById(dave.getId())).isFalse();
        assertThat(repository.existsById(carter.getId())).isFalse();
        assertThat(repository.existsById("1")).isFalse();
        assertThat(repository.existsById("2")).isFalse();

        // Restore records
        repository.save(dave);
        repository.save(carter);

        // Pre-deletion check
        assertThat(repository.existsById(dave.getId())).isTrue();
        assertThat(repository.existsById(carter.getId())).isTrue();
        assertThat(repository.existsById("1")).isFalse();
        assertThat(repository.existsById("2")).isFalse();

        // Non-existent records cause the exception, they are not ignored
        assertThatThrownBy(() -> template.deleteExistingByIds(List.of("1", dave.getId(), "2", carter.getId()), Person.class))
            .isInstanceOf(AerospikeException.BatchRecordArray.class)
            .hasMessageContaining("Batch failed");

        // Cleanup
        repository.save(dave);
        repository.save(carter);
    }

    @Test
    void deleteAll() {
        repository.deleteAll(List.of(dave, carter));
        assertThat(repository.findAllById(List.of(dave.getId(), carter.getId()))).isEmpty();
        // cleanup
        repository.save(dave);
        repository.save(carter);
    }
}
