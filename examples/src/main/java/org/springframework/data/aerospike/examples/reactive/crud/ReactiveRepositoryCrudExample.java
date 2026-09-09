package org.springframework.data.aerospike.examples.reactive.crud;

import org.springframework.stereotype.Component;

@Component
public class ReactiveRepositoryCrudExample {

    private final ReactiveMovieRepository repository;

    public ReactiveRepositoryCrudExample(ReactiveMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        ReactiveMovieDocument movie = new ReactiveMovieDocument("reactive-crud-1", "Arrival", 2016, 7.9);

        repository.save(movie).block();

        ReactiveMovieDocument loaded = repository.findById(movie.getId())
            .blockOptional()
            .orElseThrow(() -> new IllegalStateException("Saved movie was not found"));
        require("Arrival".equals(loaded.getTitle()), "Loaded movie title did not match");
        require(Boolean.TRUE.equals(repository.existsById(movie.getId()).block()), "Saved movie should exist");

        repository.deleteById(movie.getId()).block();
        require(Boolean.FALSE.equals(repository.existsById(movie.getId()).block()), "Deleted movie should not exist");

        System.out.println("Saved, loaded, checked, and deleted a reactive repository document");
    }

    private void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
