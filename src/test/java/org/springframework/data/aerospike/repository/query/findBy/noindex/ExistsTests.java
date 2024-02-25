package org.springframework.data.aerospike.repository.query.findBy.noindex;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.sample.Address;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the "Exists" repository query. Keywords: Exists.
 */
public class ExistsTests extends PersonRepositoryQueryTests {

    @Test
    void findByNestedSimplePropertyExists() {
        Assertions.assertThat(stefan.getAddress())
            .isNull();
        Assertions.assertThat(carter.getAddress()
            .getZipCode()).isNotNull();
        Assertions.assertThat(dave.getAddress()
            .getZipCode()).isNotNull();

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
    void findByPOJOExists() {
        Assertions.assertThat(stefan.getAddress())
            .isNull();
        Assertions.assertThat(carter.getAddress())
            .isNotNull();
        Assertions.assertThat(dave.getAddress())
            .isNotNull();

        Assertions.assertThat(repository.findByAddressExists())
            .contains(carter, dave)
            .doesNotContain(stefan);
    }

    @Test
    void findByPOJOExistsNegativeTest() {
        assertThatThrownBy(() -> negativeTestsRepository.findByAddressExists(new Address(null, null, null, null)))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Person.address IS_NOT_NULL: expecting no arguments");
    }
}
