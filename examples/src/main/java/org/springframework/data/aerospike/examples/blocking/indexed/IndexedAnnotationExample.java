package org.springframework.data.aerospike.examples.blocking.indexed;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class IndexedAnnotationExample {

    private final AnnotatedMovieRepository repository;

    public IndexedAnnotationExample(AnnotatedMovieRepository repository) {
        this.repository = repository;
    }

    public void run() {
        repository.saveAll(List.of(
            new AnnotatedMovieDocument("indexed-annotation-1", "Moon", "science-fiction"),
            new AnnotatedMovieDocument("indexed-annotation-2", "The Third Man", "noir")
        ));

        List<AnnotatedMovieDocument> results = repository.findByGenre("science-fiction");
        require(results.size() == 1, "Expected one movie from @Indexed-backed query");
        require("Moon".equals(results.get(0).getTitle()), "Indexed query result title did not match");

        System.out.println("Queried by a field whose secondary index was declared with @Indexed");
    }

    private void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
