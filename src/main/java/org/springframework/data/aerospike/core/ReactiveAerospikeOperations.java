package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.Serializable;
import java.util.Collection;
import java.util.Optional;
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

    <T> Flux<T> findAll(Class<T> type);

    <T> Mono<Optional<T>> findById(Serializable id, Class<T> type);

    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> type);

    <T> Flux<T> find(Query query, Class<T> type);

    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> type);

    <T> Mono<Long> count(Query query, Class<T> type);

    <T> Mono<T> execute(Supplier<T> supplier);

    Mono<Boolean> exists(Object id, Class<?> type);

    <T> Mono<Boolean> delete(T objectToDelete);

    MappingContext<?, ?> getMappingContext();

}
