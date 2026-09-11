package org.springframework.data.aerospike.examples.combined.blocking.derived;

import org.springframework.data.aerospike.examples.combined.blocking.derived.repository.BlockingDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

public class BlockingDerivedQueryDisjunctionExample {

    private final BlockingDerivedQueryRepository repository;

    public BlockingDerivedQueryDisjunctionExample(BlockingDerivedQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-derived-query-disjunction", repository);

        // One OR: the lGenre index exists, but a top-level OR widens the result set.
        // QueryContextBuilder cannot represent that as one secondary-index filter, so scans must be enabled.
        requireTitles(repository.findByGenreOrReleaseYear(of(CRIME), of(1979)),
            "One derived query disjunction should scan even with an lGenre index", "Alien", "Collateral", "Heat");

        // Multiple OR: adding title keeps OR at the top level and still produces no secondary-index filter.
        requireTitles(repository.findByGenreOrReleaseYearOrTitle(
                of(CRIME), of(1979), of("Network")),
            "Multiple derived query disjunction should scan even with an lGenre index",
            "Alien", "Collateral", "Heat", "Network");

        // Mixed derived query: Spring Data parses this method as OR(AND(genre, releaseYear), title),
        // not as AND(genre, OR(releaseYear, title)).
        // Because OR is the top-level operator, no single Aerospike secondary-index filter can be used.
        requireTitles(repository.findByGenreAndReleaseYearOrTitle(
                of(SCIENCE_FICTION), of(1979), of("Heat")),
            "Mixed derived query top-level disjunction should scan even with an lGenre index", "Alien", "Heat");

        System.out.println("Ran blocking derived query disjunctions in a scan-enabled indexed context");
    }
}
