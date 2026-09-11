package org.springframework.data.aerospike.examples.combined.blocking.dsl.repository;

import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface BlockingDeclaredQueryRepository extends AerospikeRepository<Movie, String> {

    @Query(
        expression = "$.lGenre == ?0 and $.lYear == ?1",
        indexToUse = Movie.GENRE_INDEX
    )
    List<Movie> findByGenreAndReleaseYear(String genre, int releaseYear);

    @Query(
        expression = "$.lGenre == ?0 and $.lYear == ?1 and $.lTitle == ?2",
        indexToUse = Movie.GENRE_INDEX
    )
    List<Movie> findByGenreAndReleaseYearAndTitle(
        String genre, int releaseYear, String title);

    @Query(
        expression = "$.lGenre == ?0 and ($.lTitle == 'Aliens' or $.lYear == 1979)",
        indexToUse = Movie.GENRE_INDEX
    )
    List<Movie> findByGenreAndAliensOr1979(String genre);

    @Query(expression = "$.lGenre == 'crime' or $.lYear == 1979")
    List<Movie> findCrimeOr1979();

    @Query(expression = "$.lGenre == 'crime' or $.lYear == 1979 or $.lTitle == 'Network'")
    List<Movie> findCrimeOr1979OrNetwork();

    @Query(expression = "($.lGenre == 'science-fiction' and $.lYear == 1979) or $.lTitle == 'Heat'")
    List<Movie> findScienceFiction1979OrHeat();
}
