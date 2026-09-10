package org.springframework.data.aerospike.examples.reactive.crud;

import org.springframework.data.aerospike.examples.reactive.crud.entity.ReactiveMovieDocument;
import org.springframework.data.aerospike.examples.reactive.crud.repository.ReactiveMovieRepository;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class ReactiveRepositoryCrudExample {

    private final ReactiveMovieRepository repository;

    // Spring injects a reactive repository proxy that returns Reactor publishers
    public ReactiveRepositoryCrudExample(ReactiveMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        saveAndReadSingleMovie();
        saveAndReadSeveralMovies();
        countAndFindAllMovies();
        deleteMovies();

        System.out.println("Ran inherited reactive repository CRUD methods for movie documents");
    }

    private void saveAndReadSingleMovie() {
        ReactiveMovieDocument movie = new ReactiveMovieDocument("reactive-crud-1", "Arrival", 2016, 7.9);

        // save(...) returns a Mono; block() is used here only to keep the example sequential
        ReactiveMovieDocument saved = requireValue(repository.save(movie).block(), "Saved movie should be emitted");

        // findById(...) emits the saved record by its @Id value
        ReactiveMovieDocument loaded = repository.findById(saved.getId())
            .blockOptional()
            .orElseThrow(() -> new IllegalStateException("Saved movie was not found"));

        String loadedTitle = loaded.getTitle();
        require("Arrival".equals(loadedTitle), "Loaded movie title did not match");

        // existsById(...) checks presence and returns the result asynchronously
        Boolean savedMovieExists = repository.existsById(saved.getId()).block();
        require(Boolean.TRUE.equals(savedMovieExists), "Saved movie should exist");
    }

    private void saveAndReadSeveralMovies() {
        List<ReactiveMovieDocument> movies = List.of(
            new ReactiveMovieDocument("reactive-crud-2", "The Vast of Night", 2019, 6.7),
            new ReactiveMovieDocument("reactive-crud-3", "Moon", 2009, 7.8),
            new ReactiveMovieDocument("reactive-crud-4", "Gattaca", 1997, 7.8)
        );

        // saveAll(Publisher) persists a stream of entities and emits the saved instances
        List<ReactiveMovieDocument> savedMovies = toSortedList(repository.saveAll(Flux.fromIterable(movies))
            .collectList()
            .block(), Comparator.comparing(ReactiveMovieDocument::getId), "Expected a list of movies");
        require(savedMovies.size() == 3, "Expected three saved movies");

        // findAllById(Publisher) demonstrates the reactive id-stream overload
        List<ReactiveMovieDocument> selectedMovies = toSortedList(repository
            .findAllById(Flux.just("reactive-crud-2", "reactive-crud-4"))
            .collectList()
            .block(), Comparator.comparing(ReactiveMovieDocument::getId), "Expected a list of movies");
        require(selectedMovies.size() == 2, "Expected two movies loaded by id");
        require("The Vast of Night".equals(selectedMovies.get(0).getTitle()), "First selected movie title did not match");
        require("Gattaca".equals(selectedMovies.get(1).getTitle()), "Second selected movie title did not match");
    }

    private void countAndFindAllMovies() {
        Long count = repository.count().block();
        require(Long.valueOf(4).equals(count), "Expected four movies before deletes");

        // findAll() is sorted in memory because live scan ordering is not deterministic
        List<ReactiveMovieDocument> allMovies = toSortedList(repository.findAll().collectList().block(),
            Comparator.comparing(ReactiveMovieDocument::getId), "Expected a list of movies");
        require(allMovies.size() == 4, "Expected four movies from findAll");
        require("reactive-crud-1".equals(allMovies.get(0).getId()), "First sorted movie id did not match");
        require("reactive-crud-4".equals(allMovies.get(3).getId()), "Last sorted movie id did not match");
    }

    private void deleteMovies() {
        ReactiveMovieDocument deleteByEntity = new ReactiveMovieDocument("reactive-crud-delete-entity", "Primer", 2004, 6.8);
        ReactiveMovieDocument deleteById = new ReactiveMovieDocument("reactive-crud-delete-id", "Coherence", 2013, 7.2);
        ReactiveMovieDocument deleteByIdsOne = new ReactiveMovieDocument("reactive-crud-delete-ids-1", "Upgrade", 2018, 7.5);
        ReactiveMovieDocument deleteByIdsTwo = new ReactiveMovieDocument("reactive-crud-delete-ids-2", "Possessor", 2020, 6.5);

        repository.saveAll(List.of(deleteByEntity, deleteById, deleteByIdsOne, deleteByIdsTwo))
            .collectList()
            .block();

        // delete(entity) removes the record represented by the entity instance
        repository.delete(deleteByEntity).block();
        require(Boolean.FALSE.equals(repository.existsById(deleteByEntity.getId()).block()),
            "Movie deleted by entity should not exist");

        // deleteById(Publisher) removes the record whose id is emitted by the publisher
        repository.deleteById(Flux.just(deleteById.getId())).block();
        require(Boolean.FALSE.equals(repository.existsById(deleteById.getId()).block()),
            "Movie deleted by id should not exist");

        // deleteAllById(...) removes a batch of records by id
        repository.deleteAllById(List.of(deleteByIdsOne.getId(), deleteByIdsTwo.getId())).block();
        require(Boolean.FALSE.equals(repository.existsById(deleteByIdsOne.getId()).block()),
            "First movie deleted by ids should not exist");
        require(Boolean.FALSE.equals(repository.existsById(deleteByIdsTwo.getId()).block()),
            "Second movie deleted by ids should not exist");

        // deleteAll() clears the remaining records in this example set
        repository.deleteAll().block();
        requireRepositoryEmptyAfterDeleteAll();
    }

    private void requireRepositoryEmptyAfterDeleteAll() {
        for (int attempt = 0; attempt < 20; attempt++) {
            if (Long.valueOf(0).equals(repository.count().block())) {
                return;
            }
            pauseBriefly();
        }
        throw new IllegalStateException("Repository should be empty after deleteAll");
    }

    private void pauseBriefly() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for deleteAll visibility", ex);
        }
    }

    private <T> T requireValue(T value, String message) {
        require(value != null, message);
        return value;
    }
}
