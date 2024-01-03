package org.springframework.data.aerospike.config;

import com.aerospike.client.Host;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class AerospikeSettings {

    // String of hosts separated by ',' in form of hostname1[:tlsName1][:port1],...
    // An IP address must be given in one of the following formats:
    // IPv4: xxx.xxx.xxx.xxx
    // IPv6: [xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx:xxxx]
    // IPv6: [xxxx::xxxx]
    // IPv6 addresses must be enclosed by brackets. tlsName is optional.
    String hosts;
    // Namespace
    String namespace;
    // Enable scan operation
    boolean scansEnabled = false;
    // Send user defined key in addition to hash digest on both reads and writes
    boolean sendKey = true;
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
    // Storing hosts
    Host[] hostsArray;
}
