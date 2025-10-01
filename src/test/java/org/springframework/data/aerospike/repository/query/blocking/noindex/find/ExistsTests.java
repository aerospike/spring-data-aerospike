package org.springframework.data.aerospike.repository.query.blocking.noindex.find;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Exists" repository query. Keywords: Exists.
 */
public class ExistsTests extends PersonRepositoryQueryTests {

    @Test
    void findBySimplePropertyExists() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            List<Person> result = repository.findByFirstNameExists();
            assertThat(result).containsAll(allPersons);
        }
    }

    @Test
    void findByNestedSimplePropertyExists() {
        Assertions.assertThat(stefan.getAddress()).isNull();
        Assertions.assertThat(carter.getAddress().getZipCode()).isNotNull();
        Assertions.assertThat(dave.getAddress().getZipCode()).isNotNull();

        Assertions.assertThat(repository.findByAddressZipCodeExists())
            .contains(carter, dave)
            .doesNotContain(stefan);

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        // when set to null a bin/field becomes non-existing
        Assertions.assertThat(repository.findByAddressZipCodeExists())
            .contains(carter, dave)
            .doesNotContain(stefan);

        stefan.setAddress(new Address(null, null, "zipCode", null));
        repository.save(stefan);
        Assertions.assertThat(repository.findByAddressZipCodeExists())
            .contains(carter, dave, stefan);

        stefan.setAddress(null); // cleanup
        stefan.setStringMap(null);
        repository.save(stefan);
    }

    @Test
    void findByCollectionExists() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            List<Person> result = repository.findByIntsExists();
            assertThat(result).contains(dave);
        }
    }

    @Test
    void findByNestedCollectionExists() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntsExists();

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByMapExists() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            List<Person> result = repository.findByIntMapExists();
            assertThat(result).contains(carter);
        }
    }

    @Test
    void findByNestedMapExists() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendIntMapExists();

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByPOJOExists() {
        Assertions.assertThat(stefan.getAddress()).isNull();
        Assertions.assertThat(carter.getAddress()).isNotNull();
        Assertions.assertThat(dave.getAddress()).isNotNull();

        Assertions.assertThat(repository.findByAddressExists())
            .contains(carter, dave)
            .doesNotContain(stefan);
    }

    @Test
    void findByNestedPojoExists() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            assertThat(dave.getAddress()).isNotNull();

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddressExists();

            assertThat(result).contains(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByPOJOExistsNegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByAddressExists(new Address(null, null, null, null)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address IS_NOT_NULL: expecting no arguments");
    }
}
