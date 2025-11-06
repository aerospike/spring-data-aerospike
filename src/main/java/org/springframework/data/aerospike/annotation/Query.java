package org.springframework.data.aerospike.annotation;

import com.aerospike.dsl.api.DSLParser;
import org.springframework.data.annotation.QueryAnnotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This query annotation expects a {@link DSLParser} expression. Allows using either a static DSL expression
 * with a fixed value or a dynamic DSL expression with indexed placeholders like {@code ?0}, {@code ?1} etc.
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
