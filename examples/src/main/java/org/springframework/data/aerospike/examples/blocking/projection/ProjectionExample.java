package org.springframework.data.aerospike.examples.blocking.projection;

import org.springframework.data.aerospike.examples.blocking.projection.dto.MovieSummary;
import org.springframework.data.aerospike.examples.blocking.projection.entity.ProjectedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.projection.repository.ProjectionMovieRepository;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class ProjectionExample {

    private final ProjectionMovieRepository repository;

    // Spring injects a repository proxy whose query methods can return projection types
    public ProjectionExample(ProjectionMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        ProjectedMovieDocument movie =
            new ProjectedMovieDocument("projection-1", "The Conversation", "Francis Ford Coppola", 1974, 7.8);

        // save(...) stores the full document; projection queries can read selected fields later
        ProjectedMovieDocument saved = repository.save(movie);

        // findMovieSummaryById(...) returns the DTO projection declared by the repository method
        List<MovieSummary> dtoProjection = repository.findMovieSummaryById(saved.getId());

        int dtoProjectionCount = dtoProjection.size();
        require(dtoProjectionCount == 1, "Expected one DTO projection result");

        String dtoProjectionTitle = dtoProjection.get(0).getTitle();
        require("The Conversation".equals(dtoProjectionTitle), "DTO projection title did not match");

        // findById(..., type) selects the projection target dynamically at call time
        List<MovieSummary> dynamicProjection = repository.findById(saved.getId(), MovieSummary.class);

        int dynamicProjectionCount = dynamicProjection.size();
        require(dynamicProjectionCount == 1, "Expected one dynamic projection result");

        int dynamicProjectionReleaseYear = dynamicProjection.get(0).getReleaseYear();
        require(dynamicProjectionReleaseYear == 1974, "Dynamic projection release year did not match");

        System.out.println("Loaded DTO and dynamic target-class projections from repository methods");
    }
}
