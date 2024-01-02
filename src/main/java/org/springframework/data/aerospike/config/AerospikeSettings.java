package org.springframework.data.aerospike.config;

import com.aerospike.client.Host;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

@Setter
public class AerospikeSettings {

    // Hosts separated by ',' in form of <address>:<port>
    String hosts;
    // Namespace
    @Getter
    String namespace;
    // Enable scan operation
    @Getter
    boolean scansEnabled;
    // Send user defined key in addition to hash digest on both reads and writes
    @Getter
    boolean sendKey;
    // Create secondary indexes specified using `@Indexed` annotation on startup
    @Getter
    boolean createIndexesOnStartup;
    // Automatically refresh indexes cache every <N> seconds
    @Getter
    int indexCacheRefreshSeconds;
    // Automatically refresh cached server version every <N> seconds
    @Getter
    int serverVersionRefreshSeconds;
    // Limit amount of results returned by server. Non-positive value means no limit
    @Getter
    long queryMaxRecords;
    // Maximum batch size for batch write operations
    @Getter
    int batchWriteSize;
    // Define how @Id fields (primary keys) and Map keys are stored: false - always as String,
    // true - preserve original type if supported
    @Getter
    boolean keepOriginalKeyTypes;

    public Collection<Host> getHosts() {
        if (StringUtils.hasText(hosts)) return Arrays.stream(hosts.split(","))
            .map(host -> host.split(":"))
            .map(hostArr -> new Host(hostArr[0], Integer.parseInt(hostArr[1])))
            .collect(Collectors.toList());
        return null;
    }
}
