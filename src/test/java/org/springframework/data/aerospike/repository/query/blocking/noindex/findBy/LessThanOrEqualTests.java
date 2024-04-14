package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is less than or equal" repository query. Keywords: LessThanEqual, IsLessThanEqual.
 */
public class LessThanOrEqualTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimplePropertyLessThanOrEqual_Integer() {
        alicia.setFriend(boyd);
        repository.save(alicia);
        dave.setFriend(oliver);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        leroi.setFriend(carter);
        repository.save(leroi);

        List<Person> result = repository.findByFriendAgeLessThanEqual(42);

        assertThat(result).containsExactlyInAnyOrder(dave, carter);
        TestUtils.setFriendsToNull(repository, alicia, dave, carter, leroi);
    }

    @Test
    void findByNestedSimplePropertyGreaterThan_String() {
        assertThat(carter.getAddress().getZipCode()).isEqualTo("C0124");
        assertThat(dave.getAddress().getZipCode()).isEqualTo("C0123");
        assertThat(repository.findByAddressZipCodeLessThanEqual("C0125")).containsExactlyInAnyOrder(carter, dave);
    }

    @Test
    void findByNestedCollectionLessThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsLessThanEqual(List.of(1, 2, 3, 4, 5));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedMapLessThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMapLessThanEqual(Map.of("1", 2, "3", 4, "5", 6));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedPojoLessThanOrEqual() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address = new Address("Foo Street 1", 2, "C0124", "Bar");
            assertThat(dave.getAddress()).isNotNull();

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddressLessThanEqual(address);

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }
}
