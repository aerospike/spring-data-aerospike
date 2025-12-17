package org.springframework.data.aerospike.server.version;

import com.aerospike.client.IAerospikeClient;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.util.InfoCommandUtils;

import java.lang.module.ModuleDescriptor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ServerVersionSupport {

    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_0 = ModuleDescriptor.Version.parse("6.1.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_1 = ModuleDescriptor.Version.parse("6.1.0.1");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_3_0_0 = ModuleDescriptor.Version.parse("6.3.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_7_0_0_0 = ModuleDescriptor.Version.parse("7.0.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_8_0_0_0 = ModuleDescriptor.Version.parse("8.0.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_8_1_0_0 = ModuleDescriptor.Version.parse("8.1.0.0");

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
        String fullVersionString = InfoCommandUtils.request(client, client.getCluster().getRandomNode(),
            "version");
        String versionString = fullVersionString.substring(fullVersionString.lastIndexOf(' ') + 1);

        if (ModuleDescriptor.Version.parse(versionString).compareTo(SERVER_VERSION_6_1_0_0) < 0) {
            throw new UnsupportedOperationException("Minimal supported Aerospike Server version is 6.1");
        }
        log.debug("Found server version {}", versionString);
        return versionString;
    }

    /**
     * @return true if Server version is 6.1 or greater
     */
    public boolean isSIndexCardinalitySupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_1_0_0) >= 0;
    }

    /**
     * Since Aerospike Server ver. 6.1.0.1 attempting to create a secondary index which already exists or to drop a
     * non-existing secondary index returns success/OK instead of an error.
     *
     * @return true if Server version is 6.1.0.1 or greater
     */
    public boolean isDropCreateBehaviorUpdated() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_1_0_1) >= 0;
    }

    /**
     * Since Aerospike Server ver. 6.3.0.0 find by Collection Data Types (Collection / Map / POJO) is supported.
     *
     * @return true if Server version is 6.3 or greater
     */
    public boolean isFindByCDTSupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_3_0_0) >= 0;
    }

    /**
     * @return true if Server version is 7.0 or greater
     */
    public boolean isServerVersionGtOrEq7() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_7_0_0_0) >= 0;
    }

    /**
     * @return true if Server version is 8.0 or greater
     */
    public boolean isTxnSupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_8_0_0_0) >= 0;
    }

    /**
     * @return true if Server version is 8.1 or greater
     */
    public boolean isServerVersionGtOrEq8_1() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_8_1_0_0) >= 0;
    }

}
