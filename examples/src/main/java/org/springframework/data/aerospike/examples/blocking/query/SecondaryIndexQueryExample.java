package org.springframework.data.aerospike.examples.blocking.query;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.query.entity.IndexedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.query.repository.IndexedMovieRepository;

import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

// Demonstrates a blocking repository query backed by a secondary index.
public class SecondaryIndexQueryExample {

    private final IndexedMovieRepository repository;
    private final AerospikeTemplate template;

    // Spring injects both the repository proxy and the template used for index administration
    public SecondaryIndexQueryExample(IndexedMovieRepository repository, AerospikeTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    public void run() {
        // tag::secondary-index-create-with-template[]
        // createIndex(...) creates the secondary index required by the derived query method
        template.createIndex(IndexedMovieDocument.class, IndexedMovieDocument.GENRE_INDEX, "genre", IndexType.STRING);
        // end::secondary-index-create-with-template[]

        List<IndexedMovieDocument> movies = List.of(
            new IndexedMovieDocument("query-1", "Alien", "science-fiction", 1979),
            new IndexedMovieDocument("query-2", "Aliens", "science-fiction", 1986),
            new IndexedMovieDocument("query-3", "Heat", "crime", 1995)
        );

        // saveAll(...) writes all sample records before querying the indexed bin
        repository.saveAll(movies);

        // tag::secondary-index-query-method[]
        // findByGenre(...) is a repository query method resolved from its method name
        List<IndexedMovieDocument> scienceFiction = repository.findByGenre("science-fiction");
        // end::secondary-index-query-method[]

        int scienceFictionCount = scienceFiction.size();
        require(scienceFictionCount == 2, "Expected two science-fiction movies");

        boolean everyResultMatchesGenre = scienceFiction.stream()
            .allMatch(movie -> "science-fiction".equals(movie.getGenre()));
        require(everyResultMatchesGenre, "Every result should match the indexed genre");

        System.out.println("Created a genre index and queried movies with a derived repository method");
    }
}
