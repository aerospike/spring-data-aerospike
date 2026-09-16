package org.springframework.data.aerospike.examples.combined.blocking.dsl;

import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import java.util.List;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

// Demonstrates blocking declared DSL OR queries that require scans.
public class BlockingDeclaredQueryDisjunctionExample {

    private final BlockingDeclaredQueryRepository repository;

    public BlockingDeclaredQueryDisjunctionExample(BlockingDeclaredQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-declared-query-disjunction", repository);

        // tag::combined-declared-query-disjunction-usage[]
        // One OR: Movie.GENRE_INDEX exists on Movie.GENRE_BIN (`bin_genre`) in this context, but the @Query
        // expression is top-level OR.
        // The method intentionally omits indexToUse because this shape cannot be represented by one index filter.
        // This static OR-shaped annotation declares no method arguments because the expression values are fixed.
        List<Movie> crimeOr1979 = repository.findCrimeOr1979();

        // Multiple OR: indexed metadata is still not enough for scan-free execution because every branch widens
        // the result set. The query runs as a scan-backed static filter expression.
        List<Movie> crimeOr1979OrNetwork = repository.findCrimeOr1979OrNetwork();

        // Mixed top-level OR: this expression is
        // OR(AND(Movie.GENRE_BIN, Movie.RELEASE_YEAR_BIN), Movie.TITLE_BIN).
        // Even though Movie.GENRE_BIN (`bin_genre`) is indexed, there is no single filter that can cover both OR branches.
        // The method declares no arguments because the static @Query expression controls the predicate values.
        List<Movie> scienceFiction1979OrHeat = repository.findScienceFiction1979OrHeat();
        // end::combined-declared-query-disjunction-usage[]

        requireTitles(crimeOr1979, "One declared query disjunction should scan even with Movie.GENRE_INDEX",
            "Alien", "Collateral", "Heat");
        requireTitles(crimeOr1979OrNetwork,
            "Multiple declared query disjunction should scan even with Movie.GENRE_INDEX",
            "Alien", "Collateral", "Heat", "Network");
        requireTitles(scienceFiction1979OrHeat,
            "Mixed declared query top-level disjunction should scan even with Movie.GENRE_INDEX", "Alien", "Heat");

        System.out.println("Ran blocking declared query disjunctions in a scan-enabled indexed context");
    }
}
