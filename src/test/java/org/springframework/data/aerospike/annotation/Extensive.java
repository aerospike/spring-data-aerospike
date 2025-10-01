package org.springframework.data.aerospike.annotation;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks extensive tests. Intended to run nightly.
 * <p>
 * Tests marked with {@code @Extensive} are excluded from core testing within regular CI builds ({@code mvn test})
 * and included only when specifically requesting them ({@code mvn test -Dgroups="extensive"}) or when running all tests
 * ({@code mvn clean test -Pall-tests})
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Tag("extensive")
public @interface Extensive {

}
