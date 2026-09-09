package org.springframework.data.aerospike.examples.blocking.crud;

import org.springframework.data.aerospike.examples.blocking.crud.entity.MovieDocument;
import org.springframework.data.aerospike.examples.blocking.crud.repository.MovieRepository;
import org.springframework.stereotype.Component;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class BlockingRepositoryCrudExample {

    private final MovieRepository repository;

    // Spring injects a repository proxy that implements the declared repository interface.
    public BlockingRepositoryCrudExample(MovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieDocument movie = new MovieDocument("blocking-crud-1", "Sneakers", 1992, 7.1);

        // save(...) writes the entity to the Aerospike set declared by @Document.
        MovieDocument saved = repository.save(movie);

        // findById(...) loads the saved record by its @Id value.
        MovieDocument loaded = repository.findById(saved.getId())
            .orElseThrow(() -> new IllegalStateException("Saved movie was not found"));

        String loadedTitle = loaded.getTitle();
        require("Sneakers".equals(loadedTitle), "Loaded movie title did not match");

        // existsById(...) checks whether a record is present without loading the whole entity.
        boolean savedMovieExists = repository.existsById(saved.getId());
        require(savedMovieExists, "Saved movie should exist");

        // deleteById(...) removes the record identified by the entity id.
        repository.deleteById(saved.getId());

        boolean deletedMovieExists = repository.existsById(saved.getId());
        require(!deletedMovieExists, "Deleted movie should not exist");

        System.out.println("Saved, loaded, checked, and deleted a blocking repository document");
    }
}
