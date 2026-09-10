package org.springframework.data.aerospike.examples.blocking.querymethods;

import org.springframework.data.aerospike.examples.blocking.querymethods.entity.QueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.blocking.querymethods.repository.QueryMethodsMovieRepository;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class BlockingRepositoryQueryMethodsExample {

    private final QueryMethodsMovieRepository repository;

    // Spring Data parses this repository's method names into Aerospike queries
    public BlockingRepositoryQueryMethodsExample(QueryMethodsMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
            new QueryMethodsMovieDocument("blocking-query-methods-1", "Alien", "science-fiction", 1979),
            new QueryMethodsMovieDocument("blocking-query-methods-2", "Aliens", "science-fiction", 1986),
            new QueryMethodsMovieDocument("blocking-query-methods-3", "Heat", "crime", 1995),
            new QueryMethodsMovieDocument("blocking-query-methods-4", "Collateral", "crime", 2004)
        ));

        // findByGenre(...) uses the fixture-created string index on the genre bin
        List<QueryMethodsMovieDocument> scienceFiction = toSortedList(repository.findByGenre("science-fiction"),
            Comparator.comparing(QueryMethodsMovieDocument::getId));
        require(scienceFiction.size() == 2, "Expected two science-fiction movies");
        require("Alien".equals(scienceFiction.get(0).getTitle()), "First genre query title did not match");
        require("Aliens".equals(scienceFiction.get(1).getTitle()), "Second genre query title did not match");

        // findByReleaseYearBetween(...) uses the numeric releaseYear index for a range query
        List<QueryMethodsMovieDocument> ninetiesMovies =
            toSortedList(repository.findByReleaseYearBetween(1990, 1999),
                Comparator.comparing(QueryMethodsMovieDocument::getId));
        require(ninetiesMovies.size() == 1, "Expected one movie released in the 1990s");
        require("Heat".equals(ninetiesMovies.get(0).getTitle()), "Range query title did not match");

        // existsByGenre(...) returns a boolean without exposing the matching records
        require(repository.existsByGenre("crime"), "Expected a crime movie to exist");
        require(!repository.existsByGenre("western"), "Did not expect a western movie to exist");

        // countByReleaseYearBetween(...) counts records matching the indexed range
        long eightiesAndEarlier = repository.countByReleaseYearBetween(1970, 1989);
        require(eightiesAndEarlier == 2, "Expected two movies from 1970 through 1989");

        // deleteByGenre(...) deletes every record matched by the derived query
        repository.deleteByGenre("crime");
        require(!repository.existsByGenre("crime"), "Crime movies should be deleted");
        require(repository.count() == 2, "Only science-fiction movies should remain");

        System.out.println("Ran blocking repository method-name queries for read, exists, count, and delete");
    }

}
