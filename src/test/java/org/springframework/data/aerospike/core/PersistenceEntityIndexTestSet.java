package org.springframework.data.aerospike.core;

import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.AdditionalAerospikeTestOperations;

import static org.assertj.core.api.Assertions.assertThat;

@UtilityClass
public class PersistenceEntityIndexTestSet {

    public void verifyPersistenceEntityIndexesCreated(AdditionalAerospikeTestOperations operations) {
        assertThat(operations.indexExists("auto-indexed-set_someField")).isTrue();
        assertThat(operations.indexExists("auto-indexed-set_shortName")).isTrue();
        assertThat(operations.indexExists("auto-indexed-set_someLongNamedField")).isFalse();
        assertThat(operations.indexExists("overridden_index_name")).isTrue();
        assertThat(operations.indexExists("auto-indexed-set_customIndexName")).isFalse();
        assertThat(operations.indexExists("auto-indexed-set_nonIndexed")).isFalse();
    }

}
