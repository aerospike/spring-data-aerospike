package org.springframework.data.aerospike.examples.support;

import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

public final class ExampleCollections {

    private ExampleCollections() {
    }

    public static <T> List<T> toSortedList(Iterable<T> values, Comparator<? super T> comparator) {
        return toSortedList(values, comparator, "Expected values to sort");
    }

    public static <T> List<T> toSortedList(Iterable<T> values, Comparator<? super T> comparator, String message) {
        require(values != null, message);
        return StreamSupport.stream(values.spliterator(), false)
            .sorted(comparator)
            .toList();
    }
}
