package org.springframework.data.aerospike.examples.combined.blocking.programmatic;

import org.springframework.data.aerospike.examples.combined.blocking.programmatic.repository.BlockingCustomQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.CRIME;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

public class BlockingCustomQueryNoIndexExample {

    private final BlockingCustomQueryRepository repository;

    public BlockingCustomQueryNoIndexExample(BlockingCustomQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-custom-query-no-index", repository);

        // One AND: no secondary index exists, so scans must be enabled even though AND could otherwise use one.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979)))),
            "No-index custom query conjunction should scan", "Alien");

        // One OR: top-level OR has no secondary-index filter and this context has no indexes at all.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.or(genre(CRIME), releaseYear(1979)))),
            "No-index custom query disjunction should scan", "Alien", "Collateral", "Heat");

        // Multiple AND remains expression-only because lGenre was deliberately not indexed.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.and(genre(SCIENCE_FICTION), releaseYear(1979),
                title("Alien")))),
            "No-index custom query conjunction should scan", "Alien");

        // Multiple OR remains expression-only and scan-backed.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.or(genre(CRIME), releaseYear(1979),
                title("Network")))),
            "No-index custom query disjunction should scan", "Alien", "Collateral", "Heat", "Network");

        // Mixed AND around OR can use lGenre only in the indexed example.
        // Here the same shape is a scan because no lGenre index exists.
        requireTitles(repository.findUsingQuery(new Query(Qualifier.and(genre(SCIENCE_FICTION),
                Qualifier.or(title("Aliens"), releaseYear(1979))))),
            "No-index mixed custom query conjunction should scan", "Alien", "Aliens");

        System.out.println("Ran blocking combined custom queries in a scan-enabled no-index context");
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
