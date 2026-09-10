package org.springframework.data.aerospike.examples.logical.blocking.dsl;

import org.springframework.data.aerospike.examples.logical.blocking.dsl.repository.BlockingLogicalQueryDslMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples;

import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.SCIENCE_FICTION;
import static org.springframework.data.aerospike.examples.logical.support.LogicalMovieExamples.requireTitles;

public class BlockingLogicalQueryDslIndexedANDExample {

    private final BlockingLogicalQueryDslMovieRepository repository;

    public BlockingLogicalQueryDslIndexedANDExample(BlockingLogicalQueryDslMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        LogicalMovieExamples.saveMovies("blocking-query-dsl-indexed-and", repository);

        // One AND: @Query supplies indexToUse = GENRE_INDEX, so the DSL parser can choose the lGenre
        // secondary-index filter. releaseYear stays in the filter expression.
        requireTitles(repository.findByGenreAndReleaseYear(SCIENCE_FICTION, 1979),
            "One @Query AND expression should use " + LogicalMovieDocument.GENRE_INDEX, "Alien");

        // Multiple AND: indexToUse still selects lGenre; the other two predicates are expression filters.
        requireTitles(repository.findByGenreAndReleaseYearAndTitle(
                SCIENCE_FICTION, 1979, "Alien"),
            "Multiple @Query AND expression should use " + LogicalMovieDocument.GENRE_INDEX, "Alien");

        // Mixed AND-shaped DSL: unlike the derived method with a similar name, this @Query expression is explicitly
        // AND(lGenre, OR(lTitle, lYear)). The top-level AND uses a parameterized lGenre index filter, then evaluates
        // the static nested OR expression on the indexed records.
        requireTitles(repository.findByGenreAndAliensOr1979(SCIENCE_FICTION),
            "Mixed @Query AND around OR should use " + LogicalMovieDocument.GENRE_INDEX, "Alien", "Aliens");

        System.out.println("Ran blocking @Query AND expressions backed by the lGenre index");
    }
}
