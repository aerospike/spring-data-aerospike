package org.springframework.data.aerospike.config;


import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AerospikeDataSettings {

    // Enable scan operation
    boolean scansEnabled = false;
    // Create secondary indexes specified using `@Indexed` annotation on startup
    boolean createIndexesOnStartup = true;
    // Automatically refresh indexes cache every <N> seconds
    int indexCacheRefreshSeconds = 3600;
    // Automatically refresh cached server version every <N> seconds
    int serverVersionRefreshSeconds = 3600;
    // Limit amount of results returned by server. Non-positive value means no limit
    long queryMaxRecords = 10_000L;
    // Maximum batch size for batch write operations
    int batchWriteSize = 100;
    // Define how @Id fields (primary keys) and Map keys are stored: false - always as String,
    // true - preserve original type if supported
    boolean keepOriginalKeyTypes = false;
}
