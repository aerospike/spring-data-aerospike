package org.springframework.data.aerospike.examples.blocking.customquery;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CustomQueryDslExample {

    private final CustomQueryMovieRepository repository;

    public CustomQueryDslExample(CustomQueryMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
            new CustomQueryMovieDocument("custom-query-1", "Stalker", "science-fiction", 1979),
            new CustomQueryMovieDocument("custom-query-2", "Children of Men", "science-fiction", 2006),
            new CustomQueryMovieDocument("custom-query-3", "High and Low", "crime", 1963)
        ));

        List<CustomQueryMovieDocument> modernMovies = repository.findByReleaseYearBetween(2000, 2010);
        require(modernMovies.size() == 1, "Expected one movie from the custom DSL query");
        require("Children of Men".equals(modernMovies.get(0).getTitle()), "DSL query result title did not match");

        System.out.println("Queried movies with an @Query DSL expression backed by a numeric index.");
    }

    private void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
