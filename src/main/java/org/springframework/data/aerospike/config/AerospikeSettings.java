package org.springframework.data.aerospike.config;

import lombok.Data;

@Data
public class AerospikeSettings {

    final AerospikeConnectionSettings connectionSettings;
    final AerospikeDataSettings dataSettings;
}
