package org.springframework.data.aerospike.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Mark test method as not requiring indexed bin names. Opposite to {@link AssertBinsAreIndexed}
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface NoSecondaryIndexRequired {

}
