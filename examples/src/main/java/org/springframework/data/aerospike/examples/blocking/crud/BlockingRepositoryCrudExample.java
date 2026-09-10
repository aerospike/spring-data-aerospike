package org.springframework.data.aerospike.examples.blocking.crud;

import org.springframework.data.aerospike.examples.blocking.crud.entity.MovieDocument;
import org.springframework.data.aerospike.examples.blocking.crud.repository.MovieRepository;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;

@Component
public class BlockingRepositoryCrudExample {

    private final MovieRepository repository;

    // Spring injects a repository proxy that implements the declared repository interface
    public BlockingRepositoryCrudExample(MovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        saveAndReadSingleMovie();
        saveAndReadSeveralMovies();
        countAndFindAllMovies();
        deleteMovies();

        System.out.println("Ran inherited blocking repository CRUD methods for movie documents");
    }

    private void saveAndReadSingleMovie() {
        MovieDocument movie = new MovieDocument("blocking-crud-1", "Sneakers", 1992, 7.1);

        // save(...) writes the entity to the Aerospike set declared by @Document
        MovieDocument saved = repository.save(movie);

        // findById(...) loads the saved record by its @Id value
        MovieDocument loaded = repository.findById(saved.getId())
            .orElseThrow(() -> new IllegalStateException("Saved movie was not found"));

        String loadedTitle = loaded.getTitle();
        require("Sneakers".equals(loadedTitle), "Loaded movie title did not match");

        // existsById(...) checks whether a record is present without loading the whole entity
        boolean savedMovieExists = repository.existsById(saved.getId());
        require(savedMovieExists, "Saved movie should exist");
    }

    private void saveAndReadSeveralMovies() {
        List<MovieDocument> movies = List.of(
            new MovieDocument("blocking-crud-2", "The Conversation", 1974, 7.8),
            new MovieDocument("blocking-crud-3", "The Third Man", 1949, 8.1),
            new MovieDocument("blocking-crud-4", "The Long Goodbye", 1973, 7.5)
        );

        // saveAll(...) persists a batch of entities and returns the saved instances
        List<MovieDocument> savedMovies = toSortedList(repository.saveAll(movies),
            Comparator.comparing(MovieDocument::getId));
        require(savedMovies.size() == 3, "Expected three saved movies");

        // findAllById(...) loads several known ids without relying on findAll() ordering
        List<MovieDocument> selectedMovies = toSortedList(
            repository.findAllById(List.of("blocking-crud-2", "blocking-crud-4")),
            Comparator.comparing(MovieDocument::getId));
        require(selectedMovies.size() == 2, "Expected two movies loaded by id");
        require("The Conversation".equals(selectedMovies.get(0).getTitle()), "First selected movie title did not match");
        require("The Long Goodbye".equals(selectedMovies.get(1).getTitle()), "Second selected movie title did not match");
    }

    private void countAndFindAllMovies() {
        // count() sees all records currently owned by this example's set
        long count = repository.count();
        require(count == 4, "Expected four movies before deletes");

        // findAll() is intentionally sorted in memory because Aerospike does not guarantee scan ordering
        List<MovieDocument> allMovies = toSortedList(repository.findAll(), Comparator.comparing(MovieDocument::getId));
        require(allMovies.size() == 4, "Expected four movies from findAll");
        require("blocking-crud-1".equals(allMovies.get(0).getId()), "First sorted movie id did not match");
        require("blocking-crud-4".equals(allMovies.get(3).getId()), "Last sorted movie id did not match");
    }

    private void deleteMovies() {
        MovieDocument deleteByEntity = new MovieDocument("blocking-crud-delete-entity", "Thief", 1981, 7.4);
        MovieDocument deleteById = new MovieDocument("blocking-crud-delete-id", "Ronin", 1998, 7.2);
        MovieDocument deleteByIdsOne = new MovieDocument("blocking-crud-delete-ids-1", "Charade", 1963, 7.9);
        MovieDocument deleteByIdsTwo = new MovieDocument("blocking-crud-delete-ids-2", "Klute", 1971, 7.1);

        repository.saveAll(List.of(deleteByEntity, deleteById, deleteByIdsOne, deleteByIdsTwo));

        // delete(entity) removes the record represented by the entity instance
        repository.delete(deleteByEntity);
        require(!repository.existsById(deleteByEntity.getId()), "Movie deleted by entity should not exist");

        // deleteById(...) removes one record by its id
        repository.deleteById(deleteById.getId());
        require(!repository.existsById(deleteById.getId()), "Movie deleted by id should not exist");

        // deleteAllById(...) removes a batch of records by id
        repository.deleteAllById(List.of(deleteByIdsOne.getId(), deleteByIdsTwo.getId()));
        require(!repository.existsById(deleteByIdsOne.getId()), "First movie deleted by ids should not exist");
        require(!repository.existsById(deleteByIdsTwo.getId()), "Second movie deleted by ids should not exist");

        // deleteAll() clears the remaining records in this example set
        repository.deleteAll();
        require(repository.count() == 0, "Repository should be empty after deleteAll");
    }
}
