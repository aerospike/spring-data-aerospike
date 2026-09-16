package org.springframework.data.aerospike.examples.combined.blocking.dsl.repository;

import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

// tag::combined-declared-query-repository[]
public interface BlockingDeclaredQueryRepository extends AerospikeRepository<Movie, String> {

    // tag::combined-declared-query-conjunction-methods[]
    @Query(
        expression = "$.bin_genre == ?0 and $.bin_year == ?1",
        indexToUse = Movie.GENRE_INDEX
    )
    List<Movie> findByGenreAndReleaseYear(String genre, int releaseYear);

    @Query(
        expression = "$.bin_genre == ?0 and $.bin_year == ?1 and $.bin_title == ?2",
        indexToUse = Movie.GENRE_INDEX
    )
    List<Movie> findByGenreAndReleaseYearAndTitle(
        String genre, int releaseYear, String title);

    @Query(
        expression = "$.bin_genre == ?0 and ($.bin_title == 'Aliens' or $.bin_year == 1979)",
        indexToUse = Movie.GENRE_INDEX
    )
    List<Movie> findByGenreAndAliensOr1979(String genre);
    // end::combined-declared-query-conjunction-methods[]

    // tag::combined-declared-query-disjunction-methods[]
    // tag::declared-query-static-method[]
    @Query(expression = "$.bin_genre == 'crime' or $.bin_year == 1979")
    List<Movie> findCrimeOr1979();
    // end::declared-query-static-method[]

    @Query(expression = "$.bin_genre == 'crime' or $.bin_year == 1979 or $.bin_title == 'Network'")
    List<Movie> findCrimeOr1979OrNetwork();

    @Query(expression = "($.bin_genre == 'science-fiction' and $.bin_year == 1979) or $.bin_title == 'Heat'")
    List<Movie> findScienceFiction1979OrHeat();
    // end::combined-declared-query-disjunction-methods[]
}
// end::combined-declared-query-repository[]
