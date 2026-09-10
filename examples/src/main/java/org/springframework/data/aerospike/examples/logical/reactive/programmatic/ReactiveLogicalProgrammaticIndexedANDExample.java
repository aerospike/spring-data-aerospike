package org.springframework.data.aerospike.examples.logical.reactive.programmatic;

import org.springframework.data.aerospike.examples.logical.dto.LogicalMovieSummary;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.examples.logical.reactive.programmatic.repository.ReactiveLogicalProgrammaticMovieRepository;
import org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireSummaryTitles;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireTitles;

public class ReactiveLogicalProgrammaticIndexedANDExample {

    private final ReactiveLogicalProgrammaticMovieRepository repository;

    public ReactiveLogicalProgrammaticIndexedANDExample(ReactiveLogicalProgrammaticMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        LogicalMovieExamples.saveMovies("reactive-programmatic-indexed-and", repository);

        // One AND: lGenre is the only secondary index in this context and scans are disabled.
        // QueryContextBuilder uses that index as the Aerospike filter and leaves lYear
        // as a filter expression.
        Query oneAnd = new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979)));
        requireTitles(repository.findUsingQuery(oneAnd).collectList().block(),
            "One reactive programmatic AND query should use the lGenre index", "Alien");

        // The target-class overload can still project results while using the same indexed query.
        requireSummaryTitles(repository.findUsingQuery(oneAnd, LogicalMovieSummary.class).collectList().block(),
            "Reactive programmatic AND projection should use the lGenre index", "Alien");

        // Multiple AND: lGenre remains the only available secondary-index filter.
        // The release year and title checks are evaluated as expressions after the index lookup.
        Query multipleAnd = new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979), title("Alien")));
        requireTitles(repository.findUsingQuery(multipleAnd).collectList().block(),
            "Multiple reactive programmatic AND query should use the lGenre index", "Alien");

        // Mixed no-DSL custom query: this shape is intentionally AND(lGenre, OR(...)).
        // That differs from the mixed derived method example. Because the outer operator is AND and
        // lGenre is an indexed standalone qualifier, the lGenre index can still be used.
        Query andAroundOr = new Query(Qualifier.and(genre(SCIENCE_FICTION), Qualifier.or(title("Aliens"),
            releaseYear(1979))));
        requireTitles(repository.findUsingQuery(andAroundOr).collectList().block(),
            "Reactive programmatic AND around nested OR should use the lGenre index", "Alien", "Aliens");

        System.out.println("Ran reactive programmatic AND queries backed by the lGenre index");
    }

    private static Qualifier genre(String genre) {
        return eq(LogicalMovieDocument.GENRE_BIN, genre);
    }

    private static Qualifier title(String title) {
        return eq(LogicalMovieDocument.TITLE_BIN, title);
    }

    private static Qualifier releaseYear(int releaseYear) {
        return eq(LogicalMovieDocument.RELEASE_YEAR_BIN, releaseYear);
    }

    private static Qualifier eq(String binName, Object value) {
        return Qualifier.builder()
            .setPath(binName)
            .setFilterOperation(FilterOperation.EQ)
            .setValue(value)
            .build();
    }
}
