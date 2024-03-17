package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.utility.TestUtils;

/**
 * Tests for the "Is null" repository query. Keywords: Null, IsNull.
 */
public class IsNullTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimplePropertyIsNull() {
        Assertions.assertThat(stefan.getAddress()).isNull();
        Assertions.assertThat(carter.getAddress().getZipCode()).isNotNull();
        Assertions.assertThat(dave.getAddress().getZipCode()).isNotNull();

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        Assertions.assertThat(repository.findByAddressZipCodeIsNull())
            .contains(stefan)
            .doesNotContain(carter, dave);

        dave.setBestFriend(stefan);
        repository.save(dave);
        carter.setFriend(dave);
        repository.save(carter);
        Assertions.assertThat(repository.findByFriendBestFriendAddressZipCodeIsNull()).contains(carter);

        stefan.setAddress(null); // cleanup
        repository.save(stefan);
        TestUtils.setFriendsToNull(repository, carter, dave);
    }

    @Test
    void findByPOJOIsNull() {
        Assertions.assertThat(stefan.getAddress()).isNull();
        Assertions.assertThat(carter.getAddress()).isNotNull();
        Assertions.assertThat(dave.getAddress()).isNotNull();
        Assertions.assertThat(repository.findByAddressIsNull())
            .contains(stefan)
            .doesNotContain(carter, dave);

        stefan.setAddress(new Address(null, null, null, null));
        repository.save(stefan);
        Assertions.assertThat(repository.findByAddressIsNull()).doesNotContain(stefan);

        stefan.setAddress(null); // cleanup
        repository.save(stefan);
    }
}
