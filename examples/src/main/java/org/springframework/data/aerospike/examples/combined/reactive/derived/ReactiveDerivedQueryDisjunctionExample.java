package org.springframework.data.aerospike.examples.combined.reactive.derived;

import org.springframework.data.aerospike.examples.combined.reactive.derived.repository.ReactiveDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

public class ReactiveDerivedQueryDisjunctionExample {

    private final ReactiveDerivedQueryRepository repository;

    public ReactiveDerivedQueryDisjunctionExample(ReactiveDerivedQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("reactive-derived-query-disjunction", repository);

        // One OR: the lGenre index exists, but a top-level OR widens the result set.
        // QueryContextBuilder cannot represent that as one secondary-index filter, so scans must be enabled.
        requireTitles(repository.findByGenreOrReleaseYear(of(CRIME), of(1979))
                .collectList()
                .block(),
            "One reactive derived query disjunction should scan even with an lGenre index",
            "Alien", "Collateral", "Heat");

        // Multiple OR: adding title keeps OR at the top level and still produces no secondary-index filter.
        requireTitles(repository.findByGenreOrReleaseYearOrTitle(
                    of(CRIME), of(1979), of("Network"))
                .collectList()
                .block(),
            "Multiple reactive derived query disjunction should scan even with an lGenre index",
            "Alien", "Collateral", "Heat", "Network");

        // Mixed derived query: Spring Data parses this method as OR(AND(genre, releaseYear), title),
        // not as AND(genre, OR(releaseYear, title)).
        // Because OR is the top-level operator, no single Aerospike secondary-index filter can be used.
        requireTitles(repository.findByGenreAndReleaseYearOrTitle(
                    of(SCIENCE_FICTION), of(1979), of("Heat"))
                .collectList()
                .block(),
            "Mixed reactive derived top-level OR query should scan even with an lGenre index", "Alien", "Heat");

        System.out.println("Ran reactive derived query disjunctions in a scan-enabled indexed context");
    }
}
