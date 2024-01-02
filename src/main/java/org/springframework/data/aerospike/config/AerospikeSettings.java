package org.springframework.data.aerospike.config;

import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AerospikeSettings {

    // Hosts separated by ',' in form of <address>:<port>
    String hosts;
    // Namespace
    String namespace;
    // Enable scan operation
    boolean scansEnabled;
    // Send user defined key in addition to hash digest on both reads and writes
    boolean sendKey;
    // Create secondary indexes specified using `@Indexed` annotation on startup
    boolean createIndexesOnStartup;
    // Automatically refresh indexes cache every <N> seconds
    int indexCacheRefreshSeconds;
    // Automatically refresh cached server version every <N> seconds
    int serverVersionRefreshSeconds;
    // Limit amount of results returned by server. Non-positive value means no limit
    long queryMaxRecords;
    // Maximum batch size for batch write operations
    int batchWriteSize;
    // Define how @Id fields (primary keys) and Map keys are stored: false - always as String,
    // true - preserve original type if supported
    boolean keepOriginalKeyTypes;
}
