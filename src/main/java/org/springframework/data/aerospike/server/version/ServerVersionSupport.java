package org.springframework.data.aerospike.server.version;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.lang.module.ModuleDescriptor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServerVersionSupport {

    private static final ModuleDescriptor.Version SERVER_VERSION_5_7_0_0 = ModuleDescriptor.Version.parse("5.7.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_0_0_0 = ModuleDescriptor.Version.parse("6.0.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_0 = ModuleDescriptor.Version.parse("6.1.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_1 = ModuleDescriptor.Version.parse("6.1.0.1");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_3_0_0 = ModuleDescriptor.Version.parse("6.3.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_7_0_0_0 = ModuleDescriptor.Version.parse("7.0.0.0");

    private final IAerospikeClient client;
    private final ScheduledExecutorService executorService;
    @Getter
    private volatile String serverVersion;

    public ServerVersionSupport(IAerospikeClient client) {
        this.client = client;
        this.serverVersion = findServerVersion();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
    }

    public void scheduleServerVersionRefresh(long intervalSeconds) {
        executorService.scheduleWithFixedDelay(
            () -> serverVersion = findServerVersion(),
            intervalSeconds,
            intervalSeconds,
            TimeUnit.SECONDS);
    }

    private String findServerVersion() {
//        String fullVersionString = InfoCommandUtils.request(client, client.getCluster().getRandomNode(),
//            "version");
        String fullVersionString = Info.request(client.getInfoPolicyDefault(),
            client.getCluster().getRandomNode(), "version");

        String versionString = fullVersionString.substring(fullVersionString.lastIndexOf(' ') + 1);
        log.debug("Found server version {}", versionString);
        return versionString;
    }

    public boolean isQueryShowSupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_5_7_0_0) >= 0;
    }

    public boolean isBatchWriteSupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_0_0_0) >= 0;
    }

    public boolean isSIndexCardinalitySupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_1_0_0) >= 0;
    }

    /**
     * Since Aerospike Server ver. 6.1.0.1 attempting to create a secondary index which already exists or to drop a
     * non-existing secondary index returns success/OK instead of an error.
     */
    public boolean isDropCreateBehaviorUpdated() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_1_0_1) >= 0;
    }

    /**
     * Since Aerospike Server ver. 6.3.0.0 find by Collection Data Types (Collection / Map / POJO) is supported.
     */
    public boolean isFindByCDTSupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_3_0_0) >= 0;
    }

    public boolean isServerVersionGtOrEq7() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_7_0_0_0) >= 0;
    }
}
