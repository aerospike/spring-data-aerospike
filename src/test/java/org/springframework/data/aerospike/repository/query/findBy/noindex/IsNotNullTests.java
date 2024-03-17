package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Address;

/**
 * Tests for the "Is not null" repository query. Keywords: NotNull, IsNotNull.
 */
public class IsNotNullTests extends PersonRepositoryQueryTests {

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
}
