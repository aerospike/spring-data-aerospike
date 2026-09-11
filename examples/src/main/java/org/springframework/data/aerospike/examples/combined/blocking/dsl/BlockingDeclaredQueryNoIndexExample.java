package org.springframework.data.aerospike.examples.combined.blocking.dsl;

import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

public class BlockingDeclaredQueryNoIndexExample {

    private final BlockingDeclaredQueryRepository repository;

    public BlockingDeclaredQueryNoIndexExample(BlockingDeclaredQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-declared-query-no-index", repository);

        // One AND: this method names GENRE_INDEX, but the no-index fixture drops it before the context starts.
        // With no relevant lGenre index present and scans enabled, the DSL expression runs as a scan.
        requireTitles(repository.findByGenreAndReleaseYear(SCIENCE_FICTION, 1979),
            "No-index declared query conjunction should scan", "Alien");

        // One OR: top-level OR has no secondary-index filter and this context also has no secondary indexes.
        // The static OR-shaped @Query declares no method arguments because the expression values are fixed.
        requireTitles(repository.findCrimeOr1979(),
            "No-index declared query disjunction should scan", "Alien", "Collateral", "Heat");

        // Multiple AND: all predicates are evaluated by the DSL filter expression because GENRE_INDEX is absent.
        requireTitles(repository.findByGenreAndReleaseYearAndTitle(
                SCIENCE_FICTION, 1979, "Alien"),
            "No-index declared query conjunction should scan", "Alien");

        // Multiple OR remains scan-backed and uses fixed predicate values in the annotation expression.
        requireTitles(repository.findCrimeOr1979OrNetwork(),
            "No-index declared query disjunction should scan", "Alien", "Collateral", "Heat", "Network");

        // Mixed AND-shaped DSL can use lGenre only when GENRE_INDEX exists. Here it scans.
        // The outer lGenre predicate remains parameterized while the nested OR branch uses fixed values.
        requireTitles(repository.findByGenreAndAliensOr1979(SCIENCE_FICTION),
            "No-index mixed declared query conjunction around OR should scan", "Alien", "Aliens");

        // Mixed top-level OR scans in both indexed and no-index contexts. The static @Query expression controls
        // the predicate values.
        requireTitles(repository.findScienceFiction1979OrHeat(),
            "No-index mixed declared query top-level disjunction should scan", "Alien", "Heat");

        System.out.println("Ran blocking declared query expressions in a scan-enabled no-index context");
    }
}
