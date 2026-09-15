package org.springframework.data.aerospike.examples.combined.blocking.derived;

import org.springframework.data.aerospike.examples.combined.blocking.derived.repository.BlockingDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import java.util.List;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

// Demonstrates blocking combined derived OR queries that require scans.
public class BlockingDerivedQueryDisjunctionExample {

    private final BlockingDerivedQueryRepository repository;

    public BlockingDerivedQueryDisjunctionExample(BlockingDerivedQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-derived-query-disjunction", repository);

        // tag::combined-derived-query-disjunction-usage[]
        // One OR: Movie.GENRE_INDEX exists on Movie.GENRE_BIN (`bin_genre`), but a top-level OR widens the result set.
        // QueryContextBuilder cannot represent that as one secondary-index filter, so scans must be enabled.
        List<Movie> genreOrYear = repository.findByGenreOrReleaseYear(of(CRIME), of(1979));

        // Multiple OR: adding title keeps OR at the top level and still produces no secondary-index filter.
        List<Movie> genreOrYearOrTitle = repository.findByGenreOrReleaseYearOrTitle(
            of(CRIME), of(1979), of("Network"));

        // Mixed derived query: Spring Data parses this method as
        // OR(AND(Movie.GENRE_BIN, Movie.RELEASE_YEAR_BIN), Movie.TITLE_BIN),
        // not as AND(genre, OR(releaseYear, title)).
        // Because OR is the top-level operator, no single Aerospike secondary-index filter can be used.
        List<Movie> genreAndYearOrTitle = repository.findByGenreAndReleaseYearOrTitle(
            of(SCIENCE_FICTION), of(1979), of("Heat"));
        // end::combined-derived-query-disjunction-usage[]

        requireTitles(genreOrYear, "One derived query disjunction should scan even with Movie.GENRE_INDEX",
            "Alien", "Collateral", "Heat");
        requireTitles(genreOrYearOrTitle, "Multiple derived query disjunction should scan even with Movie.GENRE_INDEX",
            "Alien", "Collateral", "Heat", "Network");
        requireTitles(genreAndYearOrTitle, "Mixed derived query top-level disjunction should scan even with Movie.GENRE_INDEX",
            "Alien", "Heat");

        System.out.println("Ran blocking derived query disjunctions in a scan-enabled indexed context");
    }
}
