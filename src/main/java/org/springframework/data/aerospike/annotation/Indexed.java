/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.springframework.data.aerospike.annotation;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a field to be indexed using Aerospike's secondary index.
 * This will make spring-data-aerospike create index on application's startup.
 * <p>
 * For more details on Secondary index feature please refer to
 * <a href="https://www.aerospike.com/docs/architecture/secondary-index.html">Aerospike Secondary index</a>.
 *
 * @author Taras Danylchuk
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexed {

    /**
     * If not set, name will be automatically generated with pattern {setName}_{fieldName}_lowercase{type}_lowercase{collectionType}.
     *
     * Allows the actual value to be set using standard Spring property sources mechanism.
     * Syntax is the same as for {@link org.springframework.core.env.Environment#resolveRequiredPlaceholders(String)}.
     * SpEL is NOT supported.
     */
    String name() default "";

    /**
     * Underlying data type of secondary index.
     */
    IndexType type();

    /**
     * Secondary index collection type.
     */
    IndexCollectionType collectionType() default IndexCollectionType.DEFAULT;
}