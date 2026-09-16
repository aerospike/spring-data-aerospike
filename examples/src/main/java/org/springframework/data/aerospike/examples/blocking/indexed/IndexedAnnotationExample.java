package org.springframework.data.aerospike.examples.blocking.indexed;

import org.springframework.data.aerospike.examples.blocking.indexed.entity.AnnotatedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.indexed.repository.AnnotatedMovieRepository;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

// Demonstrates startup secondary-index creation from @Indexed.
public class IndexedAnnotationExample {

    private final AnnotatedMovieRepository repository;

    // Spring injects a repository proxy for the document whose indexed field is annotated
    public IndexedAnnotationExample(AnnotatedMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        List<AnnotatedMovieDocument> movies = List.of(
            new AnnotatedMovieDocument("indexed-annotation-1", "Moon", "science-fiction"),
            new AnnotatedMovieDocument("indexed-annotation-2", "The Third Man", "noir")
        );

        // tag::indexed-annotation-usage[]
        // saveAll(...) writes records after startup has created the @Indexed secondary index
        repository.saveAll(movies);

        // findByGenre(...) uses a derived repository query against the annotated indexed field
        List<AnnotatedMovieDocument> results = repository.findByGenre("science-fiction");
        // end::indexed-annotation-usage[]

        int resultCount = results.size();
        require(resultCount == 1, "Expected one movie from @Indexed-backed query");

        String resultTitle = results.get(0).getTitle();
        require("Moon".equals(resultTitle), "Indexed query result title did not match");

        System.out.println("Queried by a field whose secondary index was declared with @Indexed");
    }
}
