package org.springframework.data.aerospike.examples.logical.blocking.derived;

import org.springframework.data.aerospike.examples.logical.blocking.derived.repository.BlockingLogicalDerivedMovieRepository;
import org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples;

import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireTitles;
import static org.springframework.data.aerospike.query.QueryParam.of;

public class BlockingLogicalDerivedIndexedANDExample {

    private final BlockingLogicalDerivedMovieRepository repository;

    public BlockingLogicalDerivedIndexedANDExample(BlockingLogicalDerivedMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        LogicalMovieExamples.saveMovies("blocking-derived-indexed-and", repository);

        // One AND: this context disables scans and has logical secondary indexes available.
        // For this method, lGenre is the only indexed predicate, so QueryContextBuilder uses it as the Aerospike
        // secondary-index filter and evaluates releaseYear as a filter expression on the indexed records.
        requireTitles(repository.findByGenreAndReleaseYear(of(SCIENCE_FICTION), of(1979)),
            "One derived AND query should use the lGenre index", "Alien");

        // Multiple AND: Spring Data builds a three-part derived AND method as a nested AND:
        // AND(AND(genre, releaseYear), title). The outer title branch is visible to QueryContextBuilder, so this
        // fixture also creates the lTitle index and that index becomes the secondary-index filter for this method.
        requireTitles(repository.findByGenreAndReleaseYearAndTitle(
                of(SCIENCE_FICTION), of(1979), of("Alien")),
            "Multiple derived AND query should use the lTitle index", "Alien");

        System.out.println("Ran blocking derived AND queries backed by the lGenre and lTitle indexes");
    }
}
