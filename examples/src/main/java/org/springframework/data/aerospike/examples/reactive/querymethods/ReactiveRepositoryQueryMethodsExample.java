package org.springframework.data.aerospike.examples.reactive.querymethods;

import org.springframework.data.aerospike.examples.reactive.querymethods.entity.ReactiveQueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.reactive.querymethods.repository.ReactiveQueryMethodsMovieRepository;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class ReactiveRepositoryQueryMethodsExample {

    private final ReactiveQueryMethodsMovieRepository repository;

    // Spring Data parses this repository's reactive method names into Aerospike queries
    public ReactiveRepositoryQueryMethodsExample(ReactiveQueryMethodsMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
                new ReactiveQueryMethodsMovieDocument("reactive-query-methods-1", "Arrival", "science-fiction", 2016),
                new ReactiveQueryMethodsMovieDocument("reactive-query-methods-2", "Annihilation", "science-fiction", 2018),
                new ReactiveQueryMethodsMovieDocument("reactive-query-methods-3", "Memories of Murder", "crime", 2003),
                new ReactiveQueryMethodsMovieDocument("reactive-query-methods-4", "Zodiac", "crime", 2007)
            ))
            .collectList()
            .block();

        // findByGenre(...) returns a Flux backed by the fixture-created string index
        List<ReactiveQueryMethodsMovieDocument> scienceFiction = toSortedList(repository.findByGenre("science-fiction")
            .collectList()
            .block(), Comparator.comparing(ReactiveQueryMethodsMovieDocument::getId), "Expected a list of movies");
        require(scienceFiction.size() == 2, "Expected two science-fiction movies");
        require("Arrival".equals(scienceFiction.get(0).getTitle()), "First genre query title did not match");
        require("Annihilation".equals(scienceFiction.get(1).getTitle()), "Second genre query title did not match");

        // findByReleaseYearBetween(...) uses the numeric releaseYear index for a range query
        List<ReactiveQueryMethodsMovieDocument> midAughtsMovies =
            toSortedList(repository.findByReleaseYearBetween(2000, 2005).collectList().block(),
                Comparator.comparing(ReactiveQueryMethodsMovieDocument::getId), "Expected a list of movies");
        require(midAughtsMovies.size() == 1, "Expected one movie released from 2000 through 2005");
        require("Memories of Murder".equals(midAughtsMovies.get(0).getTitle()), "Range query title did not match");

        // existsByGenre(...) and countByReleaseYearBetween(...) return Mono values
        require(Boolean.TRUE.equals(repository.existsByGenre("crime").block()), "Expected a crime movie to exist");
        require(Boolean.FALSE.equals(repository.existsByGenre("western").block()),
            "Did not expect a western movie to exist");
        require(Long.valueOf(2).equals(repository.countByReleaseYearBetween(2010, 2020).block()),
            "Expected two movies from 2010 through 2020");

        // deleteByGenre(...) completes when all records matched by the derived query are deleted
        repository.deleteByGenre("crime").block();
        require(Boolean.FALSE.equals(repository.existsByGenre("crime").block()), "Crime movies should be deleted");
        require(Long.valueOf(2).equals(repository.count().block()), "Only science-fiction movies should remain");

        System.out.println("Ran reactive repository method-name queries for read, exists, count, and delete");
    }

}
