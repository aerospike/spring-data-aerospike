package org.springframework.data.aerospike.repository.support;

import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class SimpleReactiveAerospikeRepository<T, ID> implements ReactiveAerospikeRepository<T, ID> {
    private final EntityInformation<T, ID> entityInformation;
    private final ReactiveAerospikeOperations operations;

    @Override
    public <S extends T> Mono<S> save(S entity) {
        Assert.notNull(entity, "Cannot save NULL entity");
        return operations.save(entity);
    }

    @Override
    public <S extends T> Flux<S> saveAll(Iterable<S> entities) {
        Assert.notNull(entities, "The given Iterable of entities must not be null!");
        return Flux.fromIterable(entities).flatMap(this::save);
    }

    @Override
    public <S extends T> Flux<S> saveAll(Publisher<S> entityStream) {
        Assert.notNull(entityStream, "The given Publisher of entities must not be null!");
        return Flux.from(entityStream).flatMap(this::save);
    }

    @Override
    public Mono<T> findById(ID id) {
        Assert.notNull(id, "The given id must not be null!");
        return operations.findById(id, entityInformation.getJavaType());
    }

    @Override
    public Mono<T> findById(Publisher<ID> publisher) {
        Assert.notNull(publisher, "The given publisher of Id's must not be null!");
        return Mono.from(publisher).flatMap(id -> operations.findById(id, entityInformation.getJavaType()));
    }

    @Override
    public Flux<T> findAll() {
        return operations.findAll(entityInformation.getJavaType());
    }

    @Override
    public Flux<T> findAllById(Iterable<ID> ids) {
        Assert.notNull(ids, "The given Iterable of Id's must not be null!");
        return operations.findByIds(ids, entityInformation.getJavaType());
    }

    @Override
    public Flux<T> findAllById(Publisher<ID> publisher) {
        Assert.notNull(publisher, "The given publisher of Id's must not be null!");
        return Flux.from(publisher).flatMap(id -> operations.findById(id, entityInformation.getJavaType()));
    }

    @Override
    public Mono<Boolean> existsById(ID id) {
        throw new UnsupportedOperationException("Method not implemented yet.");
    }

    @Override
    public Mono<Boolean> existsById(Publisher<ID> publisher) {
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
