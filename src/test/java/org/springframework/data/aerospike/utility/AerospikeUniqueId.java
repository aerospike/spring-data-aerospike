package org.springframework.data.aerospike.utility;

import java.util.concurrent.atomic.AtomicInteger;

public class AerospikeUniqueId {

    private static final AtomicInteger counter = new AtomicInteger();

    public static String nextId() {
        return "as-" + counter.incrementAndGet();
    }

    public static int nextIntId() {
        return counter.incrementAndGet();
    }

    public static long nextLongId() {
        return counter.incrementAndGet();
    }
}
