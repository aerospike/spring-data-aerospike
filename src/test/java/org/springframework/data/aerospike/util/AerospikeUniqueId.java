package org.springframework.data.aerospike.util;

import java.util.concurrent.atomic.AtomicLong;

public class AerospikeUniqueId {

    private static final AtomicLong counter = new AtomicLong();

    public static String nextId() {
        return "as-" + counter.incrementAndGet();
    }
}
