package org.springframework.data.aerospike.repository.support;

import org.reactivestreams.Publisher;
import org.springframework.data.aerospike.core.ReactiveAerospikeOperations;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Stub implementation of {@link ReactiveAerospikeRepository}.
 *
 * @author Igor Ermolenko
 */
public class SimpleReactiveAerospikeRepository<T, ID> implements ReactiveAerospikeRepository<T, ID> {
    private final ReactiveAerospikeOperations operations;
    private final EntityInformation<T, ID> entityInformation;

    public SimpleReactiveAerospikeRepository(EntityInformation<T, ID> entityInformation, ReactiveAerospikeOperations operations) {
        this.entityInformation = entityInformation;
        this.operations = operations;
    }

    @Override
    public <S extends T> Mono<S> save(S entity) {
        Assert.notNull(entity, "Cannot save NULL entity");
        return operations.save(entity);
    }

    @Override
    public <S extends T> Flux<S> saveAll(Iterable<S> entities) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<T> findById(ID id) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<T> findById(Publisher<ID> id) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Boolean> existsById(ID id) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Boolean> existsById(Publisher<ID> id) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Flux<T> findAll() {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Flux<T> findAllById(Iterable<ID> ids) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Flux<T> findAllById(Publisher<ID> idStream) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Long> count() {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Void> deleteById(ID id) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Void> deleteById(Publisher<ID> id) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Void> delete(T entity) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Void> deleteAll(Iterable<? extends T> entities) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Void> deleteAll(Publisher<? extends T> entityStream) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Void> deleteAll() {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }
}
