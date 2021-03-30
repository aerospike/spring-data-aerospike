package org.springframework.data.aerospike;

import org.awaitility.core.ThrowingRunnable;

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
}
