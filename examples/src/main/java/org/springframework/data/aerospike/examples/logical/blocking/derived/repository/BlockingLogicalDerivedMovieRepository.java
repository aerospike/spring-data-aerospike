package org.springframework.data.aerospike.examples.logical.blocking.derived.repository;

import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface BlockingLogicalDerivedMovieRepository extends AerospikeRepository<LogicalMovieDocument, String> {

    List<LogicalMovieDocument> findByGenreAndReleaseYear(QueryParam genre, QueryParam releaseYear);

    List<LogicalMovieDocument> findByGenreAndReleaseYearAndTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    List<LogicalMovieDocument> findByGenreOrReleaseYear(QueryParam genre, QueryParam releaseYear);

    List<LogicalMovieDocument> findByGenreOrReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);

    List<LogicalMovieDocument> findByGenreAndReleaseYearOrTitle(
        QueryParam genre, QueryParam releaseYear, QueryParam title);
}
