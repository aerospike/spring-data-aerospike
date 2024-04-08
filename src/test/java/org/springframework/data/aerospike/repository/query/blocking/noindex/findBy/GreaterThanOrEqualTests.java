package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is greater than or equal" repository query. Keywords: GreaterThanEqual, IsGreaterThanEqual.
 */
public class GreaterThanOrEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findByCollectionGreaterThanOrEqual_Set() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Set<Integer> setToCompareWith = Set.of(0, 1, 2, 3, 4);
            dave.setIntSet(setToCompareWith);
            repository.save(dave);
            assertThat(dave.getIntSet()).isEqualTo(setToCompareWith);

//            List<Person> persons = repository.findByIntSetGreaterThanEqual(setToCompareWith);
            List<Person> persons = repository.findByIntSetGreaterThanEqual(Set.of(0, 3, 4));
            assertThat(persons).contains(dave);
        }
    }

    @Test
    void findByCollectionGreaterThanOrEqual_List() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            List<Integer> listToCompareWith = List.of(1, 2);
            dave.setInts(listToCompareWith);
            repository.save(dave);
            assertThat(dave.getInts()).isEqualTo(listToCompareWith);

            List<Person> persons = repository.findByIntsGreaterThanEqual(List.of(1, 1, 1, 1, 1, 1, 10));
            assertThat(persons).contains(dave);
        }
    }

    @Test
    void findByNestedCollectionGreaterThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsGreaterThanEqual(List.of(1, 2, 3, 4));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }
}
