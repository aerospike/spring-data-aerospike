package org.springframework.data.aerospike.repository.query.blocking.count;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.blocking.PersonRepositoryQueryTests;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.QueryParam.of;

/**
 * Tests for the "Equals" repository query. Keywords: Is, Equals (or no keyword).
 */
public class EqualsTests extends PersonRepositoryQueryTests {

    @Test
    void countBySimplePropertyEquals_String() {
        long result = repository.countByFirstName("Leroi");
        assertThat(result).isEqualTo(2);

        long result1 = repository.countByFirstNameIgnoreCase("lEroi");
        assertThat(result1).isEqualTo(2);

        long result2 = repository.countByFirstNameIs("lEroi"); // another way to call the query method
        assertThat(result2).isZero();
    }

    @Test
    void countById_AND_simpleProperty() {
        QueryParam ids = of(dave.getId());
        QueryParam name = of(carter.getFirstName());
        long persons = repository.countByIdAndFirstName(ids, name);
        assertThat(persons).isZero();

        ids = of(dave.getId());
        name = of(dave.getFirstName());
        persons = repository.countByIdAndFirstName(ids, name);
        assertThat(persons).isEqualTo(1);

        ids = of(List.of(leroi.getId(), leroi2.getId(), carter.getId()));
        QueryParam firstName = of(leroi.getFirstName());
        QueryParam age = of(stefan.getAge());
        long persons4 = repository.countByIdAndFirstNameOrAge(ids, firstName, age);
        assertThat(persons4).isEqualTo(2);
    }
}
