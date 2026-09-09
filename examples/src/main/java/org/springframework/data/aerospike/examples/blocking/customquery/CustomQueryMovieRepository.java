package org.springframework.data.aerospike.examples.blocking.customquery;

import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface CustomQueryMovieRepository extends AerospikeRepository<CustomQueryMovieDocument, String> {

    @Query(
        expression = "$.releaseYear >= ?0 and $.releaseYear < ?1",
        indexToUse = CustomQueryMovieDocument.RELEASE_YEAR_INDEX
    )
    List<CustomQueryMovieDocument> findByReleaseYearBetween(int fromInclusive, int toExclusive);
}
