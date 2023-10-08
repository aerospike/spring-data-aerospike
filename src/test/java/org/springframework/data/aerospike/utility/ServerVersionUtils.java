package org.springframework.data.aerospike.utility;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;

import java.lang.module.ModuleDescriptor;

public class ServerVersionUtils {

    private static final ModuleDescriptor.Version SERVER_VERSION_6_0_0_0 = ModuleDescriptor.Version.parse("6.0.0.0");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_1 = ModuleDescriptor.Version.parse("6.1.0.1");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_3_0_0 = ModuleDescriptor.Version.parse("6.3.0.0");

    public static String getServerVersion(IAerospikeClient client) {
        String versionString = Info.request(client.getCluster().getRandomNode(), "version");
        return versionString.substring(versionString.lastIndexOf(' ') + 1);
    }

    /**
     * Since Aerospike Server ver. 6.1.0.1 attempting to create a secondary index which already exists or to drop a
     * non-existing secondary index now returns success/OK instead of an error.
     */
    public static boolean isDropCreateBehaviorUpdated(IAerospikeClient client) {
        return ModuleDescriptor.Version.parse(ServerVersionUtils.getServerVersion(client))
            .compareTo(SERVER_VERSION_6_1_0_1) >= 0;
    }

    /**
     * Since Aerospike Server ver. 6.3.0.0 find by POJO is supported.
     */
    public static boolean isFindByPojoSupported(IAerospikeClient client) {
        return ModuleDescriptor.Version.parse(ServerVersionUtils.getServerVersion(client))
            .compareTo(SERVER_VERSION_6_3_0_0) >= 0;
    }

    public static boolean isBatchWriteSupported(IAerospikeClient client) {
        return ModuleDescriptor.Version.parse(ServerVersionUtils.getServerVersion(client))
            .compareTo(SERVER_VERSION_6_0_0_0) >= 0;
    }
}
