package org.springframework.data.aerospike.examples.reactive.querymethods.repository;

import org.springframework.data.aerospike.examples.reactive.querymethods.entity.ReactiveQueryMethodsMovieDocument;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface ReactiveQueryMethodsMovieRepository
    extends ReactiveAerospikeRepository<ReactiveQueryMethodsMovieDocument, String> {

    Flux<ReactiveQueryMethodsMovieDocument> findByGenre(String genre);

    Flux<ReactiveQueryMethodsMovieDocument> findByReleaseYearBetween(int fromInclusive, int toInclusive);

    Mono<Boolean> existsByGenre(String genre);

    Mono<Long> countByReleaseYearBetween(int fromInclusive, int toInclusive);

    Mono<Void> deleteByGenre(String genre);
}
