package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is not in" repository query. Keywords: NotIn, IsNotIn.
 */
public class NotInTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyNotIn_String() {
        Collection<String> firstNames;
        firstNames = allPersons.stream().map(Person::getFirstName).collect(Collectors.toSet());
        assertThat(repository.findByFirstNameNotIn(firstNames)).isEmpty();

        firstNames = List.of("Dave", "Donny", "Carter", "Boyd", "Leroi", "Stefan", "Matias", "Douglas");
        assertThat(repository.findByFirstNameNotIn(firstNames)).containsExactlyInAnyOrder(oliver, alicia);
    }

    @Test
    void findByNestedSimplePropertyNotIn_String() {
        assertThat(carter.getAddress().getZipCode()).isEqualTo("C0124");
        assertThat(dave.getAddress().getZipCode()).isEqualTo("C0123");
        assertThat(repository.findByAddressZipCodeNotIn(List.of("C0123", "C0125"))).containsOnly(carter);
    }

    @Test
    void findByNestedCollectionNotIn() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsNotIn(List.of(List.of(0, 1, 2, 3, 4, 5, 6, 7),
                List.of(1, 2, 3), List.of(0, 1, 2, 3, 4, 5)));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedMapNotIn() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMapNotIn(List.of(Map.of("0", 1, "2", 3, "4", 5, "6", 7),
                Map.of("1", 2, "3", 4567), Map.of("0", 1, "2", 3, "4", 5)));

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedPojoNotIn() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            Address address1 = new Address("Foo Street 1", 1, "C0123", "Bar");
            Address address2 = new Address("Foo Street 1", 2, "C0124", "C0123");
            Address address3 = new Address("Foo Street 1", 23, "C0125", "Bar");
            Address address4 = new Address("Foo Street 1", 456, "C0126", "Bar");
            assertThat(carter.getAddress())
                .isNotEqualTo(address1)
                .isNotEqualTo(address2)
                .isNotEqualTo(address3)
                .isNotEqualTo(address4);

            dave.setFriend(carter);
            repository.save(dave);

            List<Person> result = repository.findByFriendAddressNotIn(List.of(address1, address2, address3, address4));

            assertThat(result).contains(dave);
            TestUtils.setFriendsToNull(repository, dave);
        }
    }
}
