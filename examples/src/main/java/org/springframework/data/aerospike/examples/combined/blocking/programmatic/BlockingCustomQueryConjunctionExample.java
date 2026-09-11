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

public class BlockingCustomQueryConjunctionExample {

    private final BlockingCustomQueryRepository repository;

    public BlockingCustomQueryConjunctionExample(BlockingCustomQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-custom-query-conjunction", repository);

        // One AND: lGenre is the only secondary index in this context and scans are disabled.
        // QueryContextBuilder uses that index as the Aerospike filter and leaves lYear
        // as a filter expression.
        Query oneAnd = new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979)));
        requireTitles(repository.findUsingQuery(oneAnd),
            "One custom query conjunction should use the lGenre index", "Alien");

        // The target-class overload can still project results while using the same indexed query.
        requireSummaryTitles(repository.findUsingQuery(oneAnd, MovieSummary.class),
            "Custom query conjunction projection should use the lGenre index", "Alien");

        // Multiple AND: lGenre remains the only available secondary-index filter.
        // The release year and title checks are evaluated as expressions after the index lookup.
        Query multipleAnd = new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979), title("Alien")));
        requireTitles(repository.findUsingQuery(multipleAnd),
            "Multiple custom query conjunction should use the lGenre index", "Alien");

        // Mixed no-DSL custom query: this shape is intentionally AND(lGenre, OR(...)).
        // That differs from the mixed derived method example. Because the outer operator is AND and
        // lGenre is an indexed standalone qualifier, the lGenre index can still be used.
        Query andAroundOr = new Query(Qualifier.and(genre(SCIENCE_FICTION), Qualifier.or(title("Aliens"),
            releaseYear(1979))));
        requireTitles(repository.findUsingQuery(andAroundOr),
            "Mixed custom query conjunction around nested OR should use the lGenre index", "Alien", "Aliens");

        System.out.println("Ran blocking custom query conjunctions backed by the lGenre index");
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
