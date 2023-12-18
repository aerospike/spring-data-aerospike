package org.springframework.data.aerospike.utility;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;

import java.lang.module.ModuleDescriptor;

public class ServerVersionUtils {

    private static final ModuleDescriptor.Version SERVER_VERSION_6_0_0_0 = ModuleDescriptor.Version.parse("6.0.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_0 = ModuleDescriptor.Version.parse("6.1.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_1 = ModuleDescriptor.Version.parse("6.1.0.1");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_3_0_0 = ModuleDescriptor.Version.parse("6.3.0.0");
    private final IAerospikeClient client;
    private final String serverVersion;

    public ServerVersionUtils(IAerospikeClient client) {
        this.client = client;
        this.serverVersion = findServerVersion();
    }

    private String findServerVersion() {
        String versionString = Info.request(client.getCluster().getRandomNode(), "version");
        return versionString.substring(versionString.lastIndexOf(' ') + 1);
    }

    public String getServerVersion() {
        return serverVersion;
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
     * Since Aerospike Server ver. 6.3.0.0 find by POJO is supported.
     */
    public boolean isFindByPojoSupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_3_0_0) >= 0;
    }

    public boolean isBatchWriteSupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_0_0_0) >= 0;
    }

    public boolean isSIndexCardinalitySupported() {
        return ModuleDescriptor.Version.parse(getServerVersion())
            .compareTo(SERVER_VERSION_6_1_0_0) >= 0;
    }
}
