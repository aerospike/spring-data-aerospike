package org.springframework.data.aerospike.annotation;

import org.springframework.data.annotation.QueryAnnotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Query annotation to define expressions for repository methods. Allows using either a fixed DSL String or
 * a String with placeholders like {@code ?0}, {@code ?1} etc.
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Documented
@QueryAnnotation
@Beta
public @interface Query {

    /**
     * Use expression DSL to define the query taking precedence over method name
     *
     * @return empty {@link String} by default.
     */
    String expression();
}
