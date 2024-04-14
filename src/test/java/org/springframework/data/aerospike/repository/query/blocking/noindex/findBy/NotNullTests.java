package org.springframework.data.aerospike.repository.query.blocking.noindex.findBy;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.repository.query.blocking.noindex.PersonRepositoryQueryTests;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.util.TestUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the "Is not null" repository query. Keywords: NotNull, IsNotNull.
 */
public class NotNullTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimpleValueIsNotNull() {
        Assertions.assertThat(stefan.getAddress()).isNull();
        Assertions.assertThat(carter.getAddress().getZipCode()).isNotNull();
        Assertions.assertThat(dave.getAddress().getZipCode()).isNotNull();

        stefan.setAddress(new Address(null, null, "zipCode", null));
        repository.save(stefan);
        Assertions.assertThat(repository.findByAddressZipCodeIsNotNull())
            .contains(stefan); // zipCode is not null

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        Assertions.assertThat(repository.findByAddressZipCodeIsNotNull()).doesNotContain(stefan); // zipCode is null

        stefan.setAddress(null); // cleanup
        repository.save(stefan);
    }

    @Test
    void findByNestedCollectionIsNotNull() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);
            assertThat(carter.getFriend().getInts()).isNotNull();
            assertThat(dave.getFriend()).isNull();

            List<Person> result = repository.findByFriendIntsIsNotNull();

            assertThat(result)
                .contains(carter)
                .doesNotContain(dave);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedMapIsNotNull() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);
            assertThat(carter.getFriend().getIntMap()).isNotNull();
            assertThat(dave.getFriend()).isNull();

            List<Person> result = repository.findByFriendIntMapIsNotNull();

            assertThat(result)
                .contains(carter)
                .doesNotContain(dave);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByPOJOIsNotNull() {
        Assertions.assertThat(stefan.getAddress()).isNull();
        Assertions.assertThat(carter.getAddress()).isNotNull();
        Assertions.assertThat(dave.getAddress()).isNotNull();
        Assertions.assertThat(repository.findByAddressIsNotNull())
            .contains(carter, dave)
            .doesNotContain(stefan);

        stefan.setAddress(new Address(null, null, "zipCode", null)); // Address is not null
        repository.save(stefan);
        Assertions.assertThat(repository.findByAddressIsNotNull()).contains(stefan);

        stefan.setAddress(new Address(null, null, null, null)); // Address is not null
        repository.save(stefan);
        Assertions.assertThat(repository.findByAddressIsNotNull()).contains(stefan);

        stefan.setAddress(null); // cleanup
        repository.save(stefan);
    }

    @Test
    void findByNestedPojoIsNotNull() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            assertThat(dave.getAddress()).isNotNull();

            carter.setFriend(dave);
            repository.save(carter);

            List<Person> result = repository.findByFriendAddressIsNotNull();

            assertThat(result)
                .contains(carter)
                .doesNotContain(dave);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }
}
