package org.springframework.data.aerospike.config;

import lombok.Data;

/**
 * Class that holds configuration settings
 */
@Data
public class AerospikeSettings {

    final AerospikeConnectionSettings connectionSettings;
    final AerospikeDataSettings dataSettings;
}
