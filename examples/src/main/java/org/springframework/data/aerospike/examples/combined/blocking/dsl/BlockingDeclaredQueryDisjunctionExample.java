package org.springframework.data.aerospike.examples.combined.blocking.dsl;

import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

public class BlockingDeclaredQueryDisjunctionExample {

    private final BlockingDeclaredQueryRepository repository;

    public BlockingDeclaredQueryDisjunctionExample(BlockingDeclaredQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-declared-query-disjunction", repository);

        // One OR: lGenre is indexed in this context, but the @Query expression is top-level OR.
        // The method intentionally omits indexToUse because this shape cannot be represented by one index filter.
        // This static OR-shaped annotation declares no method arguments because the expression values are fixed.
        requireTitles(repository.findCrimeOr1979(),
            "One declared query disjunction should scan even with an lGenre index", "Alien", "Collateral", "Heat");

        // Multiple OR: indexed metadata is still not enough for scan-free execution because every branch widens
        // the result set. The query runs as a scan-backed static filter expression.
        requireTitles(repository.findCrimeOr1979OrNetwork(),
            "Multiple declared query disjunction should scan even with an lGenre index",
            "Alien", "Collateral", "Heat", "Network");

        // Mixed top-level OR: this expression is OR(AND(lGenre, lYear), lTitle).
        // Even though lGenre is indexed, there is no single filter that can cover both OR branches.
        // The method declares no arguments because the static @Query expression controls the predicate values.
        requireTitles(repository.findScienceFiction1979OrHeat(),
            "Mixed declared query top-level disjunction should scan even with an lGenre index", "Alien", "Heat");

        System.out.println("Ran blocking declared query disjunctions in a scan-enabled indexed context");
    }
}
