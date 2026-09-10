package org.springframework.data.aerospike.examples.logical.reactive.derived;

import org.springframework.data.aerospike.examples.logical.reactive.derived.repository.ReactiveLogicalDerivedMovieRepository;
import org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples;

import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

public class ReactiveLogicalDerivedIndexedScanExample {

    private final ReactiveLogicalDerivedMovieRepository repository;

    public ReactiveLogicalDerivedIndexedScanExample(ReactiveLogicalDerivedMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        LogicalMovieExamples.saveMovies("reactive-derived-indexed-scan", repository);

        // One OR: the lGenre index exists, but a top-level OR widens the result set.
        // QueryContextBuilder cannot represent that as one secondary-index filter, so scans must be enabled.
        requireTitles(repository.findByGenreOrReleaseYear(of(CRIME), of(1979))
                .collectList()
                .block(),
            "One reactive derived OR query should scan even with an lGenre index",
            "Alien", "Collateral", "Heat");

        // Multiple OR: adding title keeps OR at the top level and still produces no secondary-index filter.
        requireTitles(repository.findByGenreOrReleaseYearOrTitle(
                    of(CRIME), of(1979), of("Network"))
                .collectList()
                .block(),
            "Multiple reactive derived OR query should scan even with an lGenre index",
            "Alien", "Collateral", "Heat", "Network");

        // Mixed derived query: Spring Data parses this method as OR(AND(genre, releaseYear), title),
        // not as AND(genre, OR(releaseYear, title)).
        // Because OR is the top-level operator, no single Aerospike secondary-index filter can be used.
        requireTitles(repository.findByGenreAndReleaseYearOrTitle(
                    of(SCIENCE_FICTION), of(1979), of("Heat"))
                .collectList()
                .block(),
            "Mixed reactive derived top-level OR query should scan even with an lGenre index", "Alien", "Heat");

        System.out.println("Ran reactive derived OR queries in a scan-enabled indexed context");
    }
}
