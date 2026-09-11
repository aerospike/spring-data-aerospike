package org.springframework.data.aerospike.examples.combined.blocking.dsl;

import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.support.MovieExamples;

import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.combined.support.MovieExamples.requireTitles;

public class BlockingDeclaredQueryConjunctionExample {

    private final BlockingDeclaredQueryRepository repository;

    public BlockingDeclaredQueryConjunctionExample(BlockingDeclaredQueryRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieExamples.saveMovies("blocking-declared-query-conjunction", repository);

        // One AND: @Query supplies indexToUse = GENRE_INDEX, so the DSL parser can choose the lGenre
        // secondary-index filter. releaseYear stays in the filter expression.
        requireTitles(repository.findByGenreAndReleaseYear(SCIENCE_FICTION, 1979),
            "One declared query conjunction should use " + Movie.GENRE_INDEX, "Alien");

        // Multiple AND: indexToUse still selects lGenre; the other two predicates are expression filters.
        requireTitles(repository.findByGenreAndReleaseYearAndTitle(
                SCIENCE_FICTION, 1979, "Alien"),
            "Multiple declared query conjunction should use " + Movie.GENRE_INDEX, "Alien");

        // Mixed AND-shaped DSL: unlike the derived method with a similar name, this @Query expression is explicitly
        // AND(lGenre, OR(lTitle, lYear)). The top-level AND uses a parameterized lGenre index filter, then evaluates
        // the static nested OR expression on the indexed records.
        requireTitles(repository.findByGenreAndAliensOr1979(SCIENCE_FICTION),
            "Mixed declared query top-level conjunction around OR should use " + Movie.GENRE_INDEX, "Alien", "Aliens");

        System.out.println("Ran blocking declared query conjunctions backed by the lGenre index");
    }
}
