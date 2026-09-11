package org.springframework.data.aerospike.examples.combined.support;

import org.springframework.data.aerospike.examples.combined.dto.MovieSummary;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

public final class MovieExamples {

    public static final String SCIENCE_FICTION = "science-fiction";
    public static final String CRIME = "crime";

    private MovieExamples() {
    }

    public static List<Movie> movies(String idPrefix) {
        return List.of(
            new Movie(idPrefix + "-1", "Alien", SCIENCE_FICTION, 1979),
            new Movie(idPrefix + "-2", "Aliens", SCIENCE_FICTION, 1986),
            new Movie(idPrefix + "-3", "Heat", CRIME, 1995),
            new Movie(idPrefix + "-4", "Collateral", CRIME, 2004),
            new Movie(idPrefix + "-5", "Network", "drama", 1976),
            new Movie(idPrefix + "-6", "Solaris", SCIENCE_FICTION, 1972)
        );
    }

    public static void saveMovies(String idPrefix, AerospikeRepository<Movie, String> repository) {
        movies(idPrefix).forEach(repository::save);
    }

    public static void saveMovies(String idPrefix, ReactiveAerospikeRepository<Movie, String> repository) {
        Flux.fromIterable(movies(idPrefix))
            .concatMap(repository::save)
            .collectList()
            .block();
    }

    public static void requireTitles(Iterable<Movie> movies, String message, String... expectedTitles) {
        List<String> actualTitles = StreamSupport.stream(movies.spliterator(), false)
            .map(Movie::getTitle)
            .sorted()
            .toList();
        require(actualTitles.equals(sorted(expectedTitles)),
            message + ". Expected " + sorted(expectedTitles) + " but got " + actualTitles);
    }

    public static void requireSummaryTitles(Iterable<MovieSummary> summaries, String message,
                                            String... expectedTitles) {
        List<String> actualTitles = StreamSupport.stream(summaries.spliterator(), false)
            .map(MovieSummary::getTitle)
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
