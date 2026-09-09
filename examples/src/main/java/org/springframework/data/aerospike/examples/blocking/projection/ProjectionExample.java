package org.springframework.data.aerospike.examples.blocking.projection;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class ProjectionExample {

    private final ProjectionMovieRepository repository;

    public ProjectionExample(ProjectionMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        ProjectedMovieDocument movie =
            new ProjectedMovieDocument("projection-1", "The Conversation", "Francis Ford Coppola", 1974, 7.8);

        repository.save(movie);

        List<MovieSummary> dtoProjection = repository.findMovieSummaryById(movie.getId());
        require(dtoProjection.size() == 1, "Expected one DTO projection result");
        require("The Conversation".equals(dtoProjection.get(0).getTitle()), "DTO projection title did not match");

        List<MovieSummary> dynamicProjection = repository.findById(movie.getId(), MovieSummary.class);
        require(dynamicProjection.size() == 1, "Expected one dynamic projection result");
        require(dynamicProjection.get(0).getReleaseYear() == 1974, "Dynamic projection release year did not match");

        System.out.println("Loaded DTO and dynamic target-class projections from repository methods");
    }

    private void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
