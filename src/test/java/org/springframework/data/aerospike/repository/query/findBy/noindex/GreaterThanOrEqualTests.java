package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Person;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is greater than or equal" repository query. Keywords: GreaterThanEqual, IsGreaterThanEqual.
 */
public class GreaterThanOrEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findByCollectionGreaterThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Set<Integer> setToCompareWith = Set.of(0, 1, 2, 3, 4);
            dave.setIntSet(setToCompareWith);
            repository.save(dave);
            assertThat(dave.getIntSet()).isEqualTo(setToCompareWith);

            List<Person> persons = repository.findByIntSetGreaterThanEqual(setToCompareWith);
            assertThat(persons).contains(dave);
        }
    }
}
