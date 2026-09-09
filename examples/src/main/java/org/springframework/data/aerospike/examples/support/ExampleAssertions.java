package org.springframework.data.aerospike.examples.support;

public final class ExampleAssertions {

    private ExampleAssertions() {
    }

    public static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
