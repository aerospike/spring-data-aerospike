package org.springframework.data.aerospike.examples.combined.blocking.dsl;

import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import java.util.List;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

// Demonstrates blocking declared DSL AND queries that name an index to use.
public class BlockingDeclaredQueryConjunctionExample {

    private final BlockingDeclaredQueryRepository repository;

    public BlockingDeclaredQueryConjunctionExample(BlockingDeclaredQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-declared-query-conjunction", repository);

        // tag::combined-declared-query-conjunction-usage[]
        // One AND: @Query supplies indexToUse = Movie.GENRE_INDEX, so the DSL parser can choose
        // Movie.GENRE_BIN (`bin_genre`)
        // secondary-index filter. releaseYear stays in the filter expression.
        List<Movie> genreAndYear = repository.findByGenreAndReleaseYear(SCIENCE_FICTION, 1979);

        // Multiple AND: indexToUse still selects bin_genre; the other two predicates are expression filters.
        List<Movie> genreYearAndTitle = repository.findByGenreAndReleaseYearAndTitle(
            SCIENCE_FICTION, 1979, "Alien");

        // Mixed AND-shaped DSL: unlike the derived method with a similar name, this @Query expression is explicitly
        // AND(Movie.GENRE_BIN, OR(Movie.TITLE_BIN, Movie.RELEASE_YEAR_BIN)). The top-level AND uses a
        // parameterized Movie.GENRE_INDEX filter, then evaluates
        // the static nested OR expression on the indexed records.
        List<Movie> genreAndAliensOr1979 = repository.findByGenreAndAliensOr1979(SCIENCE_FICTION);
        // end::combined-declared-query-conjunction-usage[]

        requireTitles(genreAndYear, "One declared query conjunction should use " + Movie.GENRE_INDEX, "Alien");
        requireTitles(genreYearAndTitle, "Multiple declared query conjunction should use " + Movie.GENRE_INDEX,
            "Alien");
        requireTitles(genreAndAliensOr1979,
            "Mixed declared query top-level conjunction around OR should use " + Movie.GENRE_INDEX, "Alien", "Aliens");

        System.out.println("Ran blocking declared query conjunctions backed by Movie.GENRE_INDEX");
    }
}
