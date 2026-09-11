package org.springframework.data.aerospike.examples.combined.reactive.derived;

import org.springframework.data.aerospike.examples.combined.reactive.derived.repository.ReactiveDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

public class ReactiveDerivedQueryNoIndexExample {

    private final ReactiveDerivedQueryRepository repository;

    public ReactiveDerivedQueryNoIndexExample(ReactiveDerivedQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("reactive-derived-query-no-index", repository);

        // No secondary index exists in this context, so even AND-shaped derived queries are scans.
        requireTitles(repository.findByGenreAndReleaseYear(of(SCIENCE_FICTION), of(1979))
                .collectList()
                .block(),
            "No-index reactive derived query conjunction should scan", "Alien");

        // Top-level OR has no secondary-index filter shape and this context also has no indexes at all.
        requireTitles(repository.findByGenreOrReleaseYear(of(CRIME), of(1979))
                .collectList()
                .block(),
            "No-index reactive derived query disjunction should scan", "Alien", "Collateral", "Heat");

        // Multiple AND is still expression-only here because no lGenre index was created.
        requireTitles(repository.findByGenreAndReleaseYearAndTitle(
                    of(SCIENCE_FICTION), of(1979), of("Alien"))
                .collectList()
                .block(),
            "No-index reactive derived query conjunction should scan", "Alien");

        // Multiple OR remains expression-only and scan-backed.
        requireTitles(repository.findByGenreOrReleaseYearOrTitle(
                    of(CRIME), of(1979), of("Network"))
                .collectList()
                .block(),
            "No-index reactive derived query disjunction should scan", "Alien", "Collateral", "Heat", "Network");

        // Derived mixed logic parses as OR(AND(genre, releaseYear), title).
        // With no indexes present, the whole expression is evaluated by a scan.
        requireTitles(repository.findByGenreAndReleaseYearOrTitle(
                    of(SCIENCE_FICTION), of(1979), of("Heat"))
                .collectList()
                .block(),
            "No-index reactive mixed derived query top-level disjunction should scan", "Alien", "Heat");

        System.out.println("Ran reactive combined derived queries in a scan-enabled no-index context");
    }
}
