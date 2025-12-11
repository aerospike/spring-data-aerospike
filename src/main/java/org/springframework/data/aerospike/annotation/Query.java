package org.springframework.data.aerospike.annotation;

import com.aerospike.client.query.Filter;
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
     * DSL expression to define the query. If used on repository query method name, it takes precedence over it.
     * A mandatory parameter
     */
    String expression();

    /**
     * The name of secondary index to use for building {@link Filter}.
     * If the index with the specified name is not found in cache, selecting is done the regular way
     * (cardinality-based or alphabetically)
     *
     * @return empty {@link String} by default.
     */
    String indexToUse() default "";
}
