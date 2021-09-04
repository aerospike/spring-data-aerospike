package org.springframework.data.aerospike.utility;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import lombok.experimental.UtilityClass;

@UtilityClass
public class VersionUtils {

    private final String FILTER_EXPRESSIONS_MINIMAL_VERSION = "5.2.0";

    public static boolean isFilterExpressionSupported(IAerospikeClient client) {
        String aerospikeVersion = getAerospikeVersion(client);
        System.out.println("-============!!!!!!!!!!!!!!!!!! AerospikeVersion: " + aerospikeVersion + "!!!=========------------");
        return compareVersions(aerospikeVersion, FILTER_EXPRESSIONS_MINIMAL_VERSION) >= 0;
    }

    public static String getAerospikeVersion(IAerospikeClient client) {
        String versionString = Info.request(null, client.getNodes()[0], "version");
        return versionString.substring(versionString.lastIndexOf(' ') + 1);
    }

    public static int compareVersions(String version1, String version2) {
        int comparisonResult = 0;

        String[] version1Splits = version1.split("\\.");
        String[] version2Splits = version2.split("\\.");
        int maxLengthOfVersionSplits = Math.max(version1Splits.length, version2Splits.length);

        for (int i = 0; i < maxLengthOfVersionSplits; i++) {
            Integer v1 = i < version1Splits.length ? Integer.parseInt(version1Splits[i]) : 0;
            Integer v2 = i < version2Splits.length ? Integer.parseInt(version2Splits[i]) : 0;
            int compare = v1.compareTo(v2);
            if (compare != 0) {
                comparisonResult = compare;
                break;
            }
        }
        return comparisonResult;
    }
}
