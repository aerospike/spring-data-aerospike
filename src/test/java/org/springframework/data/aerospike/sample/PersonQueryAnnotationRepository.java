/*
 * Copyright 2012-2021 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

/**
 * This repository contains @Query-annotated methods
 */
public interface PersonQueryAnnotationRepository<P extends Person> extends AerospikeRepository<P, String> {

    /**
     *
     * @param name Actual value is omitted as a static DSL expression with fixed parameter value is used
     * @return Result of a query "$.lastName == 'Macintire'"
     */
    @Query(expression = "$.lastName == 'Macintire'")
    List<P> findByLastName(String name);

    // Dynamic DSL String, different values can be transferred using placeholders
    @Query(expression = "$.firstName == ?0")
    List<P> findByFirstName(String name);

    @Query(expression = "$.firstName == ?0")
    List<P> findByFirstNameIgnoreCase(String name);

    @Query(expression = "$.age > ?0")
    List<P> findByAgeGreaterThan(int age);

    @Query(expression = "$.isActive.get(type: BOOL) == ?0")
    List<P> findByIsActive(boolean isActive);
}
