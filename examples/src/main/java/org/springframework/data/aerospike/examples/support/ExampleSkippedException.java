package org.springframework.data.aerospike.examples.support;

public class ExampleSkippedException extends RuntimeException {

    public ExampleSkippedException(String message) {
        super(message);
    }
}
