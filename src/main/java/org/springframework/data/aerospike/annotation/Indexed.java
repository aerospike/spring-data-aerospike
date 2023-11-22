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
 * Marks a field to be indexed using Aerospike's secondary index. This will make spring-data-aerospike create index on
 * application's startup.
 * <p>
 * For more details on Secondary index feature please refer to
 * <a href="https://www.aerospike.com/docs/architecture/secondary-index.html">Aerospike Secondary index</a>
 * and to <a href="https://aerospike.github.io/spring-data-aerospike/#indexed-annotation">indexed-annotation</a>.
 * <p>
 *
 * @author Taras Danylchuk
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface Indexed {

    /**
     * If not set, name will be automatically generated with pattern
     * {setName}_{fieldName}_lowercase{type}_lowercase{collectionType}.
     * <p>
     * Allows the actual value to be set using standard Spring property sources mechanism. Syntax is the same as for
     * {@link org.springframework.core.env.Environment#resolveRequiredPlaceholders(String)}. SpEL is NOT supported.
     */
    String name() default "";

    /**
     * If bin name is not provided, the annotated field name will be used.
     */
    String bin() default "";

    /**
     * Underlying data type of secondary index.
     */
    IndexType type();

    /**
     * Secondary index collection type.
     */
    IndexCollectionType collectionType() default IndexCollectionType.DEFAULT;

    /**
     * Context is provided using the following DSL.
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> a </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> "a" </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> "1" </td> <td> Map key (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> 1 </td> <td> Map key (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {1} </td> <td> Map index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=1} </td> <td> Map value (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=bb} </td> <td> Map value “bb” </td>
     *   </tr>
     *   <tr>
     *     <td> {="1"} </td> <td> Map value (String) “1” </td>
     *   </tr>
     *     <td> {#1} </td> <td> Map rank 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [1] </td> <td> List index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [=1] </td> <td> List value 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [#1] </td> <td> List rank 1 </td>
     * </table>
     * <br>
     * Examples of complex contexts:
     * <table border="1">
     *   <tr>
     *     <td> a.aa.aaa </td> <td> [mapKey("a"), mapKey("aa"), mapKey("aaa")] </td>
     *   </tr>
     *   <tr>
     *     <td> a.55 </td> <td> [mapKey("a"), mapKey(55)] </td>
     *   </tr>
     *   <tr>
     *     <td> a.aa.{=222} </td> <td> [mapKey("a"), mapKey("aa"),mapValue(222)] </td>
     *   </tr>
     *   <tr>
     *     <td> ab.cd.[-1]."10" </td> <td> [mapKey("ab"), mapKey("cd"),listIndex(-1), mapKey("10")] </td>
     *   </tr>
     * </table>
     */
    String ctx() default "";
}
