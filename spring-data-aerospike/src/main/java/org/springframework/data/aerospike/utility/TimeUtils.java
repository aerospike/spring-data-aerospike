package org.springframework.data.aerospike.utility;

import java.util.concurrent.TimeUnit;

public final class TimeUtils {

    private TimeUtils() {
    }

    public static long offsetInSecondsToUnixTime(int expirationInSeconds) {
        return System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(expirationInSeconds);
    }

    public static int unixTimeToOffsetInSeconds(long unixTime) {
        double delta = (unixTime - System.currentTimeMillis());
        return (int) Math.round(delta / 1000);
    }
}
