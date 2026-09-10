package org.springframework.data.aerospike.examples.logical.blocking.programmatic;

import org.springframework.data.aerospike.examples.logical.blocking.programmatic.repository.BlockingLogicalProgrammaticMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireTitles;

public class BlockingLogicalProgrammaticIndexedScanExample {

    private final BlockingLogicalProgrammaticMovieRepository repository;

    public BlockingLogicalProgrammaticIndexedScanExample(BlockingLogicalProgrammaticMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        LogicalMovieExamples.saveMovies("blocking-programmatic-indexed-scan", repository);

        // One OR: lGenre is indexed, but top-level Qualifier.or(...) produces no secondary-index filter.
        // Scans are enabled here so Aerospike can evaluate the expression against the set.
        Query oneOr = new Query(Qualifier.or(genre(CRIME), releaseYear(1979)));
        requireTitles(repository.findUsingQuery(oneOr),
            "One programmatic OR query should scan even with an lGenre index", "Alien", "Collateral", "Heat");

        // Multiple OR: every predicate is part of a widening OR, so the lGenre index is not a query filter.
        Query multipleOr = new Query(Qualifier.or(genre(CRIME), releaseYear(1979), title("Network")));
        requireTitles(repository.findUsingQuery(multipleOr),
            "Multiple programmatic OR query should scan even with an lGenre index",
            "Alien", "Collateral", "Heat", "Network");

        // Mixed top-level OR: OR(AND(lGenre, lYear), lTitle) also has no single secondary-index filter,
        // even though lGenre is indexed.
        Query orAroundAnd = new Query(Qualifier.or(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979)),
            title("Heat")));
        requireTitles(repository.findUsingQuery(orAroundAnd),
            "Programmatic top-level OR around AND should scan even with an lGenre index", "Alien", "Heat");

        System.out.println("Ran blocking programmatic OR queries in a scan-enabled indexed context");
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
