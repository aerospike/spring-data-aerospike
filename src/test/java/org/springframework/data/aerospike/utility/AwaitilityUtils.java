package org.springframework.data.aerospike.utility;

import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.Assertions;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.TEN_SECONDS;
import static org.awaitility.Durations.TWO_SECONDS;

public class AwaitilityUtils {

    public static void awaitTenSecondsUntil(ThrowingRunnable runnable) {
        await().atMost(TEN_SECONDS)
            .untilAsserted(runnable);
    }

    public static void awaitTwoSecondsUntil(ThrowingRunnable runnable) {
        await().atMost(TWO_SECONDS)
            .untilAsserted(runnable);
    }

    public static void wait(long delay, TimeUnit units) {
        await()
            .timeout(delay + 1, TimeUnit.SECONDS)
            .pollDelay(delay, TimeUnit.SECONDS)
            .untilAsserted(() -> Assertions.assertTrue(true));
    }

    public static void wait(long delay, long timeout, TimeUnit units) {
        await()
            .timeout(timeout, TimeUnit.SECONDS)
            .pollDelay(delay, TimeUnit.SECONDS)
            .untilAsserted(() -> Assertions.assertTrue(true));
    }
}
