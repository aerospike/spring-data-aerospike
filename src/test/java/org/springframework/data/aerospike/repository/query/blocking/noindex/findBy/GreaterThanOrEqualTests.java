package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Is greater than or equal" repository query. Keywords: GreaterThanEqual, IsGreaterThanEqual.
 */
public class GreaterThanOrEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimplePropertyGreaterThan_String() {
        String zipCode = "C0124";
        assertThat(carter.getAddress().getZipCode()).isEqualTo(zipCode);
        assertThat(repository.findByAddressZipCodeGreaterThanEqual(zipCode)).containsExactly(carter);
    }

    @Test
    void findByCollectionGreaterThanOrEqual_Set_NegativeTest() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Set<Integer> setToCompareWith = Set.of(0, 1, 2, 3, 4);
            dave.setIntSet(setToCompareWith);
            repository.save(dave);

            // only Lists can be compared because they maintain ordering
            assertThatThrownBy(() -> negativeTestsRepository.findByIntSetGreaterThanEqual(setToCompareWith))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Person.intSet GTEQ: only Lists can be compared");
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

    @Test
    void findByNestedMapGreaterThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMapGreaterThanEqual(Map.of("1", 2, "3", 4));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedPojoGreaterThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 1, "C0123", "Bar");
            assertThat(carter.getAddress()).isNotNull();

            dave.setFriend(carter);
            repository.save(dave);

            List<Person> result = repository.findByFriendAddressGreaterThanEqual(address);

            assertThat(result).contains(dave);
            TestUtils.setFriendsToNull(repository, dave);
        }
    }
}
