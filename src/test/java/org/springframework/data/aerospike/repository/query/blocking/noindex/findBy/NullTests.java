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
 * Tests for the "Is null" repository query. Keywords: Null, IsNull.
 */
public class NullTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimplePropertyIsNull() {
        assertThat(stefan.getAddress()).isNull();
        assertThat(carter.getAddress().getZipCode()).isNotNull();
        assertThat(dave.getAddress().getZipCode()).isNotNull();

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        assertThat(repository.findByAddressZipCodeIsNull())
            .contains(stefan)
            .doesNotContain(carter, dave);

        dave.setBestFriend(stefan);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        assertThat(repository.findByFriendBestFriendAddressZipCodeIsNull()).contains(carter);

        stefan.setAddress(null); // cleanup
        repository.save(stefan);
        TestUtils.setFriendsToNull(repository, carter, dave);
    }

    @Test
    void findByNestedCollectionIsNull() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setInts(List.of(1, 2, 3, 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);
            assertThat(carter.getFriend().getInts()).isNotNull();
            assertThat(dave.getFriend()).isNull();

            List<Person> result = repository.findByFriendIntsIsNull();

            assertThat(result)
                .contains(dave)
                .doesNotContain(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByNestedMapIsNull() {
        if (serverVersionSupport.isFindByCDTSupported()) {
            dave.setIntMap(Map.of("1", 2, "3", 4));
            repository.save(dave);

            carter.setFriend(dave);
            repository.save(carter);
            assertThat(carter.getFriend().getIntMap()).isNotNull();
            assertThat(dave.getFriend()).isNull();

            List<Person> result = repository.findByFriendIntMapIsNull();

            assertThat(result)
                .contains(dave)
                .doesNotContain(carter);
            TestUtils.setFriendsToNull(repository, carter);
        }
    }

    @Test
    void findByPOJOIsNull() {
        assertThat(stefan.getAddress()).isNull();
        assertThat(carter.getAddress()).isNotNull();
        assertThat(dave.getAddress()).isNotNull();
        assertThat(repository.findByAddressIsNull())
            .contains(stefan)
            .doesNotContain(carter, dave);

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        assertThat(repository.findByAddressIsNull()).doesNotContain(stefan);

        stefan.setAddress(null); // cleanup
        repository.save(stefan);
    }
}
