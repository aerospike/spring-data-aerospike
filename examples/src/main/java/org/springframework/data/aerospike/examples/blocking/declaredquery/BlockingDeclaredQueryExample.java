package org.springframework.data.aerospike.examples.blocking.declaredquery;

import org.springframework.data.aerospike.examples.blocking.declaredquery.entity.DeclaredQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.declaredquery.repository.DeclaredQueryMovieRepository;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

// Demonstrates blocking declared repository queries using Aerospike DSL expressions.
public class BlockingDeclaredQueryExample {

    private final DeclaredQueryMovieRepository repository;

    // Spring injects a repository proxy whose method carries the declared @Query expression
    public BlockingDeclaredQueryExample(DeclaredQueryMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        List<DeclaredQueryMovieDocument> movies = List.of(
            new DeclaredQueryMovieDocument("declared-query-1", "Stalker", "science-fiction", 1979),
            new DeclaredQueryMovieDocument("declared-query-2", "Children of Men", "science-fiction", 2006),
            new DeclaredQueryMovieDocument("declared-query-3", "High and Low", "crime", 1963)
        );

        // saveAll(...) writes sample records before the declared query runs
        repository.saveAll(movies);

        // tag::declared-query-usage[]
        // findByReleaseYearBetween(...) executes the @Query DSL expression declared on the repository method
        List<DeclaredQueryMovieDocument> modernMovies = repository.findByReleaseYearBetween(2000, 2010);
        // end::declared-query-usage[]

        int modernMovieCount = modernMovies.size();
        require(modernMovieCount == 1, "Expected one movie from the declared DSL query");

        String modernMovieTitle = modernMovies.get(0).getTitle();
        require("Children of Men".equals(modernMovieTitle), "DSL query result title did not match");

        System.out.println("Queried movies with an @Query DSL expression backed by a numeric index");
    }
}
