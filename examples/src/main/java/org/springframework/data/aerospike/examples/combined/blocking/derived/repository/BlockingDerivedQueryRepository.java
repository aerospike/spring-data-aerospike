package org.springframework.data.aerospike.examples.combined.blocking.derived.repository;

import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface BlockingDerivedQueryRepository extends AerospikeRepository<Movie, String> {

    List<Movie> findByGenreAndReleaseYear(QueryParam genre, QueryParam releaseYear);

    List<Movie> findByGenreAndReleaseYearAndTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    List<Movie> findByGenreOrReleaseYear(QueryParam genre, QueryParam releaseYear);

    List<Movie> findByGenreOrReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    List<Movie> findByGenreAndReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);
}
