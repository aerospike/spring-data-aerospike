package org.springframework.data.aerospike.examples.logical.reactive.derived.repository;

import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import reactor.core.publisher.Flux;

public interface ReactiveLogicalDerivedMovieRepository extends ReactiveAerospikeRepository<LogicalMovieDocument, String> {

    Flux<LogicalMovieDocument> findByGenreAndReleaseYear(QueryParam genre, QueryParam releaseYear);

    Flux<LogicalMovieDocument> findByGenreAndReleaseYearAndTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    Flux<LogicalMovieDocument> findByGenreOrReleaseYear(QueryParam genre, QueryParam releaseYear);

    Flux<LogicalMovieDocument> findByGenreOrReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    Flux<LogicalMovieDocument> findByGenreAndReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);
}
