package org.springframework.data.aerospike.examples.logical.blocking.dsl.repository;

import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface BlockingLogicalQueryDslMovieRepository extends AerospikeRepository<LogicalMovieDocument, String> {

    @Query(
        expression = "$.lGenre == ?0 and $.lYear == ?1",
        indexToUse = LogicalMovieDocument.GENRE_INDEX
    )
    List<LogicalMovieDocument> findByGenreAndReleaseYear(String genre, int releaseYear);

    @Query(
        expression = "$.lGenre == ?0 and $.lYear == ?1 and $.lTitle == ?2",
        indexToUse = LogicalMovieDocument.GENRE_INDEX
    )
    List<LogicalMovieDocument> findByGenreAndReleaseYearAndTitle(
        String genre, int releaseYear, String title);

    @Query(
        expression = "$.lGenre == ?0 and ($.lTitle == 'Aliens' or $.lYear == 1979)",
        indexToUse = LogicalMovieDocument.GENRE_INDEX
    )
    List<LogicalMovieDocument> findByGenreAndAliensOr1979(String genre);

    @Query(expression = "$.lGenre == 'crime' or $.lYear == 1979")
    List<LogicalMovieDocument> findCrimeOr1979();

    @Query(expression = "$.lGenre == 'crime' or $.lYear == 1979 or $.lTitle == 'Network'")
    List<LogicalMovieDocument> findCrimeOr1979OrNetwork();

    @Query(expression = "($.lGenre == 'science-fiction' and $.lYear == 1979) or $.lTitle == 'Heat'")
    List<LogicalMovieDocument> findScienceFiction1979OrHeat();
}
