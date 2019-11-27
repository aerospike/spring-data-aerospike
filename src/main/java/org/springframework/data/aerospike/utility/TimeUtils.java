package org.springframework.data.aerospike.utility;

import lombok.experimental.UtilityClass;

import java.util.concurrent.TimeUnit;

@UtilityClass
public final class TimeUtils {

    public static long offsetInSecondsToUnixTime(int expirationInSeconds) {
        return System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(expirationInSeconds);
    }

    public static int unixTimeToOffsetInSeconds(long unixTime) {
        double delta = (unixTime - System.currentTimeMillis());
        return (int) Math.round(delta / 1000);
    }
}
