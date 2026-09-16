package org.springframework.data.aerospike.examples.combined.blocking.derived;

import org.springframework.data.aerospike.examples.combined.blocking.derived.repository.BlockingDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import java.util.List;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

// Demonstrates blocking combined derived AND queries that can use secondary indexes.
public class BlockingDerivedQueryConjunctionExample {

    private final BlockingDerivedQueryRepository repository;

    public BlockingDerivedQueryConjunctionExample(BlockingDerivedQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-derived-query-conjunction", repository);

        // tag::combined-derived-query-conjunction-usage[]
        // One AND: this context disables scans and has secondary indexes available.
        // For this method, Movie.GENRE_BIN (`bin_genre`) is the only indexed predicate, so QueryContextBuilder
        // uses it as the Aerospike
        // secondary-index filter and evaluates releaseYear as a filter expression on the indexed records.
        List<Movie> genreAndYear = repository.findByGenreAndReleaseYear(of(SCIENCE_FICTION), of(1979));

        // Multiple AND: Spring Data builds a three-part derived AND method as a nested AND:
        // AND(AND(genre, releaseYear), title). The outer title branch is visible to QueryContextBuilder, so this
        // fixture also creates Movie.TITLE_INDEX on Movie.TITLE_BIN (`bin_title`), and that index becomes the
        // secondary-index filter for this method.
        List<Movie> genreYearAndTitle = repository.findByGenreAndReleaseYearAndTitle(
            of(SCIENCE_FICTION), of(1979), of("Alien"));
        // end::combined-derived-query-conjunction-usage[]

        requireTitles(genreAndYear, "One derived query conjunction should use the bin_genre index", "Alien");
        requireTitles(genreYearAndTitle, "Multiple derived query conjunction should use the bin_title index", "Alien");

        System.out.println("Ran blocking derived query conjunctions backed by Movie.GENRE_INDEX and Movie.TITLE_INDEX");
    }
}
