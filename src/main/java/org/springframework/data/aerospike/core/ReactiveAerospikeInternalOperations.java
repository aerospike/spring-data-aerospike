package org.springframework.data.aerospike.core;

import org.springframework.data.aerospike.query.Qualifier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface ReactiveAerospikeInternalOperations {

    /**
     *
     * @param id
     * @param entityClass
     * @param targetClass
     * @param qualifiers
     * @return
     * @param <T>
     * @param <S>
     */
    <T, S> Mono<S> findByIdInternal(Object id, Class<T> entityClass, Class<S> targetClass,
                                    Qualifier... qualifiers);

    /**
     *
     * @param ids
     * @param entityClass
     * @param targetClass
     * @param qualifiers
     * @return
     * @param <T>
     * @param <S>
     */
    <T, S> Flux<S> findByIdsInternal(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                     Qualifier... qualifiers);
}
