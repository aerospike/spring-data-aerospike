package org.springframework.data.aerospike.examples.combined.blocking.dsl;

import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import java.util.List;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

// Demonstrates blocking declared DSL queries in a scan-enabled no-index context.
public class BlockingDeclaredQueryNoIndexExample {

    private final BlockingDeclaredQueryRepository repository;

    public BlockingDeclaredQueryNoIndexExample(BlockingDeclaredQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-declared-query-no-index", repository);

        // tag::combined-declared-query-no-index-usage[]
        // One AND: this method names Movie.GENRE_INDEX, but the no-index fixture drops it before the context starts.
        // With no relevant Movie.GENRE_INDEX present and scans enabled, the DSL expression runs as a scan.
        List<Movie> genreAndYear = repository.findByGenreAndReleaseYear(SCIENCE_FICTION, 1979);

        // One OR: top-level OR has no secondary-index filter and this context also has no secondary indexes.
        // The static OR-shaped @Query declares no method arguments because the expression values are fixed.
        List<Movie> crimeOr1979 = repository.findCrimeOr1979();

        // Multiple AND: all predicates are evaluated by the DSL filter expression because Movie.GENRE_INDEX is absent.
        List<Movie> genreYearAndTitle = repository.findByGenreAndReleaseYearAndTitle(
            SCIENCE_FICTION, 1979, "Alien");

        // Multiple OR remains scan-backed and uses fixed predicate values in the annotation expression.
        List<Movie> crimeOr1979OrNetwork = repository.findCrimeOr1979OrNetwork();

        // Mixed AND-shaped DSL can use Movie.GENRE_BIN (`bin_genre`) only when Movie.GENRE_INDEX exists.
        // Here it scans.
        // The outer Movie.GENRE_BIN (`bin_genre`) predicate remains parameterized while the nested OR branch uses
        // fixed values.
        List<Movie> genreAndAliensOr1979 = repository.findByGenreAndAliensOr1979(SCIENCE_FICTION);

        // Mixed top-level OR scans in both indexed and no-index contexts. The static @Query expression controls
        // the predicate values.
        List<Movie> scienceFiction1979OrHeat = repository.findScienceFiction1979OrHeat();
        // end::combined-declared-query-no-index-usage[]

        requireTitles(genreAndYear, "No-index declared query conjunction should scan", "Alien");
        requireTitles(crimeOr1979, "No-index declared query disjunction should scan",
            "Alien", "Collateral", "Heat");
        requireTitles(genreYearAndTitle, "No-index declared query conjunction should scan", "Alien");
        requireTitles(crimeOr1979OrNetwork, "No-index declared query disjunction should scan",
            "Alien", "Collateral", "Heat", "Network");
        requireTitles(genreAndAliensOr1979, "No-index mixed declared query conjunction around OR should scan",
            "Alien", "Aliens");
        requireTitles(scienceFiction1979OrHeat, "No-index mixed declared query top-level disjunction should scan",
            "Alien", "Heat");

        System.out.println("Ran blocking declared query expressions in a scan-enabled no-index context");
    }
}
