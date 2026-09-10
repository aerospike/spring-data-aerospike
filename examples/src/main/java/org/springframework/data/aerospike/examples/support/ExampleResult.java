package org.springframework.data.aerospike.examples.support;

import java.time.Duration;

public record ExampleResult(String name, ExampleStatus status, Duration duration, String message, Throwable cause) {

    public static ExampleResult passed(String name, Duration duration, String message) {
        return new ExampleResult(name, ExampleStatus.PASSED, duration, message, null);
    }

    public static ExampleResult skipped(String name, String message) {
        return new ExampleResult(name, ExampleStatus.SKIPPED, Duration.ZERO, message, null);
    }

    public static ExampleResult skipped(String name, Duration duration, String message) {
        return new ExampleResult(name, ExampleStatus.SKIPPED, duration, message, null);
    }

    public static ExampleResult failed(String name, Duration duration, Throwable cause) {
        return new ExampleResult(name, ExampleStatus.FAILED, duration, cause.getMessage(), cause);
    }
}
