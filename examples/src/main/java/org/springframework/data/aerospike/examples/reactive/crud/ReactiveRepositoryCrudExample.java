package org.springframework.data.aerospike.examples.reactive.crud;

import org.springframework.data.aerospike.examples.reactive.crud.entity.ReactiveMovieDocument;
import org.springframework.data.aerospike.examples.reactive.crud.repository.ReactiveMovieRepository;
import org.springframework.stereotype.Component;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

@Component
public class ReactiveRepositoryCrudExample {

    private final ReactiveMovieRepository repository;

    // Spring injects a reactive repository proxy that returns Reactor publishers.
    public ReactiveRepositoryCrudExample(ReactiveMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        ReactiveMovieDocument movie = new ReactiveMovieDocument("reactive-crud-1", "Arrival", 2016, 7.9);

        // save(...) returns a Mono; block() is used here only to keep the example sequential.
        ReactiveMovieDocument saved = repository.save(movie).block();

        // findById(...) emits the saved record by its @Id value.
        ReactiveMovieDocument loaded = repository.findById(saved.getId())
            .blockOptional()
            .orElseThrow(() -> new IllegalStateException("Saved movie was not found"));

        String loadedTitle = loaded.getTitle();
        require("Arrival".equals(loadedTitle), "Loaded movie title did not match");

        // existsById(...) checks presence and returns the result asynchronously.
        Boolean savedMovieExists = repository.existsById(saved.getId()).block();
        require(Boolean.TRUE.equals(savedMovieExists), "Saved movie should exist");

        // deleteById(...) removes the record and completes when the delete finishes.
        repository.deleteById(saved.getId()).block();

        Boolean deletedMovieExists = repository.existsById(saved.getId()).block();
        require(Boolean.FALSE.equals(deletedMovieExists), "Deleted movie should not exist");

        System.out.println("Saved, loaded, checked, and deleted a reactive repository document");
    }
}
