package org.springframework.data.aerospike.examples.logical.support;

import org.springframework.data.aerospike.examples.logical.dto.LogicalMovieSummary;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

public final class LogicalMovieExamples {

    public static final String SCIENCE_FICTION = "science-fiction";
    public static final String CRIME = "crime";

    private LogicalMovieExamples() {
    }

    public static List<LogicalMovieDocument> movies(String idPrefix) {
        return List.of(
            new LogicalMovieDocument(idPrefix + "-1", "Alien", SCIENCE_FICTION, 1979),
            new LogicalMovieDocument(idPrefix + "-2", "Aliens", SCIENCE_FICTION, 1986),
            new LogicalMovieDocument(idPrefix + "-3", "Heat", CRIME, 1995),
            new LogicalMovieDocument(idPrefix + "-4", "Collateral", CRIME, 2004),
            new LogicalMovieDocument(idPrefix + "-5", "Network", "drama", 1976),
            new LogicalMovieDocument(idPrefix + "-6", "Solaris", SCIENCE_FICTION, 1972)
        );
    }

    public static void saveMovies(String idPrefix, AerospikeRepository<LogicalMovieDocument, String> repository) {
        movies(idPrefix).forEach(repository::save);
    }

    public static void saveMovies(String idPrefix, ReactiveAerospikeRepository<LogicalMovieDocument, String> repository) {
        Flux.fromIterable(movies(idPrefix))
            .concatMap(repository::save)
            .collectList()
            .block();
    }

    public static void requireTitles(Iterable<LogicalMovieDocument> movies, String message, String... expectedTitles) {
        List<String> actualTitles = StreamSupport.stream(movies.spliterator(), false)
            .map(LogicalMovieDocument::getTitle)
            .sorted()
            .toList();
        require(actualTitles.equals(sorted(expectedTitles)),
            message + ". Expected " + sorted(expectedTitles) + " but got " + actualTitles);
    }

    public static void requireSummaryTitles(Iterable<LogicalMovieSummary> summaries, String message,
                                            String... expectedTitles) {
        List<String> actualTitles = StreamSupport.stream(summaries.spliterator(), false)
            .map(LogicalMovieSummary::getTitle)
            .sorted()
            .toList();
        require(actualTitles.equals(sorted(expectedTitles)),
            message + ". Expected " + sorted(expectedTitles) + " but got " + actualTitles);
    }

    private static List<String> sorted(String... values) {
        return Arrays.stream(values)
            .sorted()
            .toList();
    }
}
