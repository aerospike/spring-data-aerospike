package org.springframework.data.aerospike.examples.combined.blocking.programmatic;

import org.springframework.data.aerospike.examples.combined.blocking.programmatic.repository.BlockingCustomQueryRepository;
import org.springframework.data.aerospike.examples.combined.dto.MovieSummary;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireSummaryTitles;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

// Demonstrates blocking custom AND queries that can use a secondary-index filter.
public class BlockingCustomQueryConjunctionExample {

    private final BlockingCustomQueryRepository repository;

    public BlockingCustomQueryConjunctionExample(BlockingCustomQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-custom-query-conjunction", repository);

        // One AND: Movie.GENRE_INDEX on Movie.GENRE_BIN (`bin_genre`) is the only secondary index in this
        // context and scans are disabled.
        // QueryContextBuilder uses that index as the Aerospike filter and leaves Movie.RELEASE_YEAR_BIN (`bin_year`)
        // as a filter expression.
        // tag::combined-programmatic-custom-query-conjunction[]
        Query oneAnd = new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979)));
        // end::combined-programmatic-custom-query-conjunction[]
        requireTitles(repository.findUsingQuery(oneAnd),
            "One custom query conjunction should use the bin_genre index", "Alien");

        // The target-class overload can still project results while using the same indexed query.
        requireSummaryTitles(repository.findUsingQuery(oneAnd, MovieSummary.class),
            "Custom query conjunction projection should use the bin_genre index", "Alien");

        // Multiple AND: Movie.GENRE_INDEX remains the only available secondary-index filter.
        // The release year and title checks are evaluated as expressions after the index lookup.
        Query multipleAnd = new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979), title("Alien")));
        requireTitles(repository.findUsingQuery(multipleAnd),
            "Multiple custom query conjunction should use the bin_genre index", "Alien");

        // Mixed no-DSL custom query: this shape is intentionally AND(Movie.GENRE_BIN, OR(...)).
        // That differs from the mixed derived method example. Because the outer operator is AND and
        // Movie.GENRE_BIN is an indexed standalone qualifier, so Movie.GENRE_INDEX can still be used.
        // tag::combined-programmatic-custom-query-and-around-or[]
        Query andAroundOr = new Query(Qualifier.and(genre(SCIENCE_FICTION), Qualifier.or(title("Aliens"),
            releaseYear(1979))));
        // end::combined-programmatic-custom-query-and-around-or[]
        requireTitles(repository.findUsingQuery(andAroundOr),
            "Mixed custom query conjunction around nested OR should use the bin_genre index", "Alien", "Aliens");

        System.out.println("Ran blocking custom query conjunctions backed by Movie.GENRE_INDEX");
    }

    private static Qualifier genre(String genre) {
        return eq(Movie.GENRE_BIN, genre);
    }

    private static Qualifier title(String title) {
        return eq(Movie.TITLE_BIN, title);
    }

    private static Qualifier releaseYear(int releaseYear) {
        return eq(Movie.RELEASE_YEAR_BIN, releaseYear);
    }

    private static Qualifier eq(String binName, Object value) {
        return Qualifier.builder()
            .setPath(binName)
            .setFilterOperation(FilterOperation.EQ)
            .setValue(value)
            .build();
    }
}
