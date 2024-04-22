package org.springframework.data.aerospike.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Assert that the given bin names are indexed. Opposite to {@link NoSecondaryIndexRequired}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AssertBinsAreIndexed {

    String[] binNames();

    Class<?> entityClass();
}
