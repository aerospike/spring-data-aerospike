package org.springframework.data.aerospike.examples.combined.reactive.programmatic;

import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.reactive.programmatic.repository.ReactiveCustomQueryRepository;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

public class ReactiveCustomQueryDisjunctionExample {

    private final ReactiveCustomQueryRepository repository;

    public ReactiveCustomQueryDisjunctionExample(ReactiveCustomQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("reactive-custom-query-disjunction", repository);

        // One OR: lGenre is indexed, but top-level Qualifier.or(...) produces no secondary-index filter.
        // Scans are enabled here so Aerospike can evaluate the expression against the set.
        Query oneOr = new Query(Qualifier.or(genre(CRIME), releaseYear(1979)));
        requireTitles(repository.findUsingQuery(oneOr).collectList().block(),
            "One reactive custom query disjunction should scan even with an lGenre index",
            "Alien", "Collateral", "Heat");

        // Multiple OR: every predicate is part of a widening OR, so the lGenre index is not a query filter.
        Query multipleOr = new Query(Qualifier.or(genre(CRIME), releaseYear(1979), title("Network")));
        requireTitles(repository.findUsingQuery(multipleOr).collectList().block(),
            "Multiple reactive custom query disjunction should scan even with an lGenre index",
            "Alien", "Collateral", "Heat", "Network");

        // Mixed top-level OR: OR(AND(lGenre, lYear), lTitle) also has no single secondary-index filter,
        // even though lGenre is indexed.
        Query orAroundAnd = new Query(Qualifier.or(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979)),
            title("Heat")));
        requireTitles(repository.findUsingQuery(orAroundAnd).collectList().block(),
            "Reactive programmatic top-level OR around AND should scan even with an lGenre index",
            "Alien", "Heat");

        System.out.println("Ran reactive custom query disjunctions in a scan-enabled indexed context");
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
