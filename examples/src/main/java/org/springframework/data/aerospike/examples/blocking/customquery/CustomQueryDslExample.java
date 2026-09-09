package org.springframework.data.aerospike.examples.blocking.customquery;

import org.springframework.data.aerospike.examples.blocking.customquery.entity.CustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.repository.CustomQueryMovieRepository;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class CustomQueryDslExample {

    private final CustomQueryMovieRepository repository;

    // Spring injects a repository proxy whose method carries the custom @Query expression.
    public CustomQueryDslExample(CustomQueryMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        List<CustomQueryMovieDocument> movies = List.of(
            new CustomQueryMovieDocument("custom-query-1", "Stalker", "science-fiction", 1979),
            new CustomQueryMovieDocument("custom-query-2", "Children of Men", "science-fiction", 2006),
            new CustomQueryMovieDocument("custom-query-3", "High and Low", "crime", 1963)
        );

        // saveAll(...) writes sample records before the custom query runs.
        repository.saveAll(movies);

        // findByReleaseYearBetween(...) executes the @Query DSL expression declared on the repository method.
        List<CustomQueryMovieDocument> modernMovies = repository.findByReleaseYearBetween(2000, 2010);

        int modernMovieCount = modernMovies.size();
        require(modernMovieCount == 1, "Expected one movie from the custom DSL query");

        String modernMovieTitle = modernMovies.get(0).getTitle();
        require("Children of Men".equals(modernMovieTitle), "DSL query result title did not match");

        System.out.println("Queried movies with an @Query DSL expression backed by a numeric index.");
    }
}
