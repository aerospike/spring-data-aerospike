package org.springframework.data.aerospike.examples.logical.blocking.dsl;

import org.springframework.data.aerospike.examples.logical.blocking.dsl.repository.BlockingLogicalQueryDslMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples;

import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireTitles;

public class BlockingLogicalQueryDslNoIndexExample {

    private final BlockingLogicalQueryDslMovieRepository repository;

    public BlockingLogicalQueryDslNoIndexExample(BlockingLogicalQueryDslMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        LogicalMovieExamples.saveMovies("blocking-query-dsl-no-index", repository);

        // One AND: this method names GENRE_INDEX, but the no-index fixture drops it before the context starts.
        // With no relevant lGenre index present and scans enabled, the DSL expression runs as a scan.
        requireTitles(repository.findByGenreAndReleaseYear(SCIENCE_FICTION, 1979),
            "No-index @Query one AND expression should scan", "Alien");

        // One OR: top-level OR has no secondary-index filter and this context also has no logical indexes.
        // The static OR-shaped @Query declares no method arguments because the expression values are fixed.
        requireTitles(repository.findCrimeOr1979(),
            "No-index @Query one OR expression should scan", "Alien", "Collateral", "Heat");

        // Multiple AND: all predicates are evaluated by the DSL filter expression because GENRE_INDEX is absent.
        requireTitles(repository.findByGenreAndReleaseYearAndTitle(
                SCIENCE_FICTION, 1979, "Alien"),
            "No-index @Query multiple AND expression should scan", "Alien");

        // Multiple OR remains scan-backed and uses fixed predicate values in the annotation expression.
        requireTitles(repository.findCrimeOr1979OrNetwork(),
            "No-index @Query multiple OR expression should scan", "Alien", "Collateral", "Heat", "Network");

        // Mixed AND-shaped DSL can use lGenre only when GENRE_INDEX exists. Here it scans.
        // The outer lGenre predicate remains parameterized while the nested OR branch uses fixed values.
        requireTitles(repository.findByGenreAndAliensOr1979(SCIENCE_FICTION),
            "No-index @Query mixed AND around OR should scan", "Alien", "Aliens");

        // Mixed top-level OR scans in both indexed and no-index contexts. The static @Query expression controls
        // the predicate values.
        requireTitles(repository.findScienceFiction1979OrHeat(),
            "No-index @Query mixed top-level OR should scan", "Alien", "Heat");

        System.out.println("Ran blocking @Query logical expressions in a scan-enabled no-index context");
    }
}
