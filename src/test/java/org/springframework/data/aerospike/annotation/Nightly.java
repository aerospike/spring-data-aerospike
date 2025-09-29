package org.springframework.data.aerospike.annotation;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks tests that are intended for nightly runs.
 * <p>
 * Tests marked with {@code @Nightly} will be excluded from regular CI builds ({@code mvn test})
 * and included only when specifically requesting them ({@code mvn test -Dgroups="nightly"}) or when running all tests
 * ({@code mvn clean test -Pall-tests})
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Tag("nightly")
public @interface Nightly {

}
