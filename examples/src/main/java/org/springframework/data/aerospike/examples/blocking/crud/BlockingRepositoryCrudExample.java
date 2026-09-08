package org.springframework.data.aerospike.examples.blocking.crud;

import org.springframework.stereotype.Component;

@Component
public class BlockingRepositoryCrudExample {

    private final MovieRepository repository;

    public BlockingRepositoryCrudExample(MovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        MovieDocument movie = new MovieDocument("blocking-crud-1", "Sneakers", 1992, 7.1);

        repository.save(movie);

        MovieDocument loaded = repository.findById(movie.getId())
            .orElseThrow(() -> new IllegalStateException("Saved movie was not found"));
        require("Sneakers".equals(loaded.getTitle()), "Loaded movie title did not match");
        require(repository.existsById(movie.getId()), "Saved movie should exist");

        repository.deleteById(movie.getId());
        require(!repository.existsById(movie.getId()), "Deleted movie should not exist");

        System.out.println("Saved, loaded, checked, and deleted a blocking repository document");
    }

    private void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
