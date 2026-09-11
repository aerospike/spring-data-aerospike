package org.springframework.data.aerospike.examples.combined.reactive.derived.repository;

import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import reactor.core.publisher.Flux;

public interface ReactiveDerivedQueryRepository extends ReactiveAerospikeRepository<Movie, String> {

    Flux<Movie> findByGenreAndReleaseYear(QueryParam genre, QueryParam releaseYear);

    Flux<Movie> findByGenreAndReleaseYearAndTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    Flux<Movie> findByGenreOrReleaseYear(QueryParam genre, QueryParam releaseYear);

    Flux<Movie> findByGenreOrReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    Flux<Movie> findByGenreAndReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);
}
