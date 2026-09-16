package org.springframework.data.aerospike.examples.combined.blocking.derived;

import org.springframework.data.aerospike.examples.combined.blocking.derived.repository.BlockingDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import java.util.List;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

// Demonstrates blocking combined derived queries in a scan-enabled no-index context.
public class BlockingDerivedQueryNoIndexExample {

    private final BlockingDerivedQueryRepository repository;

    public BlockingDerivedQueryNoIndexExample(BlockingDerivedQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-derived-query-no-index", repository);

        // tag::combined-derived-query-no-index-usage[]
        // No secondary index exists in this context, so even AND-shaped derived queries are scans.
        List<Movie> genreAndYear = repository.findByGenreAndReleaseYear(of(SCIENCE_FICTION), of(1979));

        // Top-level OR has no secondary-index filter shape and this context also has no indexes at all.
        List<Movie> genreOrYear = repository.findByGenreOrReleaseYear(of(CRIME), of(1979));

        // Multiple AND is still expression-only here because no Movie.GENRE_INDEX was created on
        // Movie.GENRE_BIN (`bin_genre`).
        List<Movie> genreYearAndTitle = repository.findByGenreAndReleaseYearAndTitle(
            of(SCIENCE_FICTION), of(1979), of("Alien"));

        // Multiple OR remains expression-only and scan-backed.
        List<Movie> genreOrYearOrTitle = repository.findByGenreOrReleaseYearOrTitle(
            of(CRIME), of(1979), of("Network"));

        // Derived mixed logic parses as OR(AND(genre, releaseYear), title).
        // With no indexes present, the whole expression is evaluated by a scan.
        List<Movie> genreAndYearOrTitle = repository.findByGenreAndReleaseYearOrTitle(
            of(SCIENCE_FICTION), of(1979), of("Heat"));
        // end::combined-derived-query-no-index-usage[]

        requireTitles(genreAndYear, "No-index derived query conjunction should scan", "Alien");
        requireTitles(genreOrYear, "No-index derived query disjunction should scan", "Alien", "Collateral", "Heat");
        requireTitles(genreYearAndTitle, "No-index derived query conjunction should scan", "Alien");
        requireTitles(genreOrYearOrTitle, "No-index derived query disjunction should scan",
            "Alien", "Collateral", "Heat", "Network");
        requireTitles(genreAndYearOrTitle, "No-index mixed derived query should scan", "Alien", "Heat");

        System.out.println("Ran blocking combined derived queries in a scan-enabled no-index context");
    }
}
