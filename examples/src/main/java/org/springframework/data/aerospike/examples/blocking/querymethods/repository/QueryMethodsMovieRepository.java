package org.springframework.data.aerospike.examples.blocking.querymethods.repository;

import org.springframework.data.aerospike.examples.blocking.querymethods.entity.QueryMethodsMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface QueryMethodsMovieRepository extends AerospikeRepository<QueryMethodsMovieDocument, String> {

    List<QueryMethodsMovieDocument> findByGenre(String genre);

    List<QueryMethodsMovieDocument> findByReleaseYearBetween(int fromInclusive, int toInclusive);

    boolean existsByGenre(String genre);

    long countByReleaseYearBetween(int fromInclusive, int toInclusive);

    void deleteByGenre(String genre);
}
