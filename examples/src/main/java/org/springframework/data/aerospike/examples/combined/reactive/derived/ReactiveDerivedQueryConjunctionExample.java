package org.springframework.data.aerospike.examples.combined.reactive.derived;

import org.springframework.data.aerospike.examples.combined.reactive.derived.repository.ReactiveDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

// Demonstrates reactive combined derived AND queries that can use secondary indexes.
public class ReactiveDerivedQueryConjunctionExample {

    private final ReactiveDerivedQueryRepository repository;

    public ReactiveDerivedQueryConjunctionExample(ReactiveDerivedQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("reactive-derived-query-conjunction", repository);

        // One AND: this context disables scans and has secondary indexes available.
        // For this method, Movie.GENRE_BIN (`bin_genre`) is the only indexed predicate, so QueryContextBuilder
        // uses it as the Aerospike
        // secondary-index filter and evaluates releaseYear as a filter expression on the indexed records.
        requireTitles(repository.findByGenreAndReleaseYear(of(SCIENCE_FICTION), of(1979))
                .collectList()
                .block(),
            "One reactive derived query conjunction should use the bin_genre index", "Alien");

        // Multiple AND: Spring Data builds a three-part derived AND method as a nested AND:
        // AND(AND(genre, releaseYear), title). The outer title branch is visible to QueryContextBuilder, so this
        // fixture also creates Movie.TITLE_INDEX on Movie.TITLE_BIN (`bin_title`), and that index becomes the
        // secondary-index filter for this method.
        requireTitles(repository.findByGenreAndReleaseYearAndTitle(
                    of(SCIENCE_FICTION), of(1979), of("Alien"))
                .collectList()
                .block(),
            "Multiple reactive derived query conjunction should use the bin_title index", "Alien");

        System.out.println("Ran reactive derived query conjunctions backed by Movie.GENRE_INDEX and Movie.TITLE_INDEX");
    }
}
