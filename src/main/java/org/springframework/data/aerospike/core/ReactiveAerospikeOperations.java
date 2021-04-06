/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.aerospike.core;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Aerospike specific data access operations to work with reactive API
 *
 * @author Igor Ermolenko
 */
public interface ReactiveAerospikeOperations {

    <T> Mono<T> save(T document);

    <T> Flux<T> insertAll(Collection<? extends T> documents);

    <T> Mono<T> insert(T document);

    <T> Mono<T> update(T document);

    <T> Mono<T> add(T objectToAddTo, Map<String, Long> values);

    <T> Mono<T> add(T objectToAddTo, String binName, long value);

    <T> Mono<T> append(T objectToAppendTo, Map<String, String> values);

    <T> Mono<T> append(T objectToAppendTo, String binName, String value);

    <T> Mono<T> prepend(T objectToPrependTo, Map<String, String> values);

    <T> Mono<T> prepend(T objectToPrependTo, String binName, String value);

    <T> Flux<T> findAll(Class<T> entityClass);

    <T> Mono<T> findById(Object id, Class<T> entityClass);

    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> entityClass);

    <T> Flux<T> find(Query query, Class<T> entityClass);

    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

    <T> Mono<Long> count(Query query, Class<T> entityClass);

    <T> Mono<T> execute(Supplier<T> supplier);

    <T> Mono<Boolean> exists(Object id, Class<T> entityClass);

    <T> Mono<Boolean> delete(Object id, Class<T> entityClass);

    <T> Mono<Boolean> delete(T objectToDelete);

    MappingContext<?, ?> getMappingContext();

    /**
     * Creates index by specified name in Aerospike.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                                      String binName, IndexType indexType);

    /**
     * Creates index by specified name in Aerospike.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName, String binName,
                               IndexType indexType, IndexCollectionType indexCollectionType);

    /**
     * Deletes index by specified name from Aerospike.
     */
    <T> Mono<Void> deleteIndex(Class<T> entityClass, String indexName);

    IAerospikeReactorClient getAerospikeReactorClient();
}
