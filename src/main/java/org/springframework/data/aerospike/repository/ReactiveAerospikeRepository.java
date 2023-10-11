/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.repository;

import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

/**
 * Aerospike specific {@link Repository} interface with reactive support.
 *
 * @author Igor Ermolenko
 */
public interface ReactiveAerospikeRepository<T, ID> extends ReactiveCrudRepository<T, ID> {

    /**
     * Run a query to find entities by providing {@link Qualifier}s.
     * <p>
     * If multiple qualifiers are given, they are combined using AND.
     * <p>
     * Each qualifier itself may contain internal qualifiers and combine them using either {@link FilterOperation#AND}
     * or {@link FilterOperation#OR}.
     *
     * @param qualifiers One or more qualifiers representing expressions.
     * @return Flux of entities.
     */
    Flux<T> findByQualifiers(Qualifier... qualifiers);
}
