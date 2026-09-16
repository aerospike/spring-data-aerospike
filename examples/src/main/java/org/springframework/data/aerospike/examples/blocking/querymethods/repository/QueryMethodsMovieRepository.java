package org.springframework.data.aerospike.examples.blocking.querymethods.repository;

import org.springframework.data.aerospike.examples.blocking.querymethods.entity.QueryMethodsMovieDocument;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

// tag::blocking-query-methods-repository[]
public interface QueryMethodsMovieRepository extends AerospikeRepository<QueryMethodsMovieDocument, String> {

    List<QueryMethodsMovieDocument> findByGenre(String genre);

    List<QueryMethodsMovieDocument> findByReleaseYearBetween(int fromInclusive, int toInclusive);

    // tag::blocking-derived-query-id-bin-methods[]
    List<QueryMethodsMovieDocument> findByIdAndGenre(QueryParam ids, QueryParam genre);
    // end::blocking-derived-query-id-bin-methods[]

    boolean existsByGenre(String genre);

    long countByReleaseYearBetween(int fromInclusive, int toInclusive);

    void deleteByGenre(String genre);
}
// end::blocking-query-methods-repository[]
