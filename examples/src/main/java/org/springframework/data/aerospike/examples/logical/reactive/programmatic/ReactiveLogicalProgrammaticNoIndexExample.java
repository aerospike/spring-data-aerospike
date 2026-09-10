package org.springframework.data.aerospike.examples.logical.reactive.programmatic;

import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.examples.logical.reactive.programmatic.repository.ReactiveLogicalProgrammaticMovieRepository;
import org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireTitles;

public class ReactiveLogicalProgrammaticNoIndexExample {

    private final ReactiveLogicalProgrammaticMovieRepository repository;

    public ReactiveLogicalProgrammaticNoIndexExample(ReactiveLogicalProgrammaticMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        LogicalMovieExamples.saveMovies("reactive-programmatic-no-index", repository);

        // One AND: no secondary index exists, so scans must be enabled even though AND could otherwise use one.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979))))
                .collectList()
                .block(),
            "No-index reactive programmatic one AND query should scan", "Alien");

        // One OR: top-level OR has no secondary-index filter and this context has no indexes at all.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.or(genre(CRIME), releaseYear(1979))))
                .collectList()
                .block(),
            "No-index reactive programmatic one OR query should scan", "Alien", "Collateral", "Heat");

        // Multiple AND remains expression-only because lGenre was deliberately not indexed.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979),
                    title("Alien"))))
                .collectList()
                .block(),
            "No-index reactive programmatic multiple AND query should scan", "Alien");

        // Multiple OR remains expression-only and scan-backed.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.or(genre(CRIME), releaseYear(1979),
                    title("Network"))))
                .collectList()
                .block(),
            "No-index reactive programmatic multiple OR query should scan",
            "Alien", "Collateral", "Heat", "Network");

        // Mixed AND around OR can use lGenre only in the indexed example.
        // Here the same shape is a scan because no lGenre index exists.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.and(genre(SCIENCE_FICTION),
                    Qualifier.or(title("Aliens"), releaseYear(1979)))))
                .collectList()
                .block(),
            "No-index reactive programmatic mixed query should scan", "Alien", "Aliens");

        System.out.println("Ran reactive programmatic logical queries in a scan-enabled no-index context");
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
