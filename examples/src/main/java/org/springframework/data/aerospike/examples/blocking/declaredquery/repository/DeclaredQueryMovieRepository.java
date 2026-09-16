package org.springframework.data.aerospike.examples.blocking.declaredquery.repository;

import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.aerospike.examples.blocking.declaredquery.entity.DeclaredQueryMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

// tag::declared-query-repository[]
public interface DeclaredQueryMovieRepository extends AerospikeRepository<DeclaredQueryMovieDocument, String> {

    // tag::declared-query-method[]
    @Query(
        expression = "$.releaseYear >= ?0 and $.releaseYear < ?1",
        indexToUse = DeclaredQueryMovieDocument.RELEASE_YEAR_INDEX
    )
    List<DeclaredQueryMovieDocument> findByReleaseYearBetween(int fromInclusive, int toExclusive);
    // end::declared-query-method[]
}
// end::declared-query-repository[]
