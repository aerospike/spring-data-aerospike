package org.springframework.data.aerospike.examples.blocking.query;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SecondaryIndexQueryExample {

    private final IndexedMovieRepository repository;
    private final AerospikeTemplate template;

    public SecondaryIndexQueryExample(IndexedMovieRepository repository, AerospikeTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    public void run() {
        resetExampleData();
        template.createIndex(IndexedMovieDocument.class, IndexedMovieDocument.GENRE_INDEX, "genre", IndexType.STRING);

        repository.saveAll(List.of(
            new IndexedMovieDocument("query-1", "Alien", "science-fiction", 1979),
            new IndexedMovieDocument("query-2", "Aliens", "science-fiction", 1986),
            new IndexedMovieDocument("query-3", "Heat", "crime", 1995)
        ));

        List<IndexedMovieDocument> scienceFiction = repository.findByGenre("science-fiction");
        require(scienceFiction.size() == 2, "Expected two science-fiction movies");
        require(scienceFiction.stream().allMatch(movie -> "science-fiction".equals(movie.getGenre())),
            "Every result should match the indexed genre");

        System.out.println("Created a genre index and queried movies with a derived repository method");
    }

    private void resetExampleData() {
        template.deleteAll(IndexedMovieDocument.class);
        try {
            template.deleteIndex(IndexedMovieDocument.class, IndexedMovieDocument.GENRE_INDEX);
        } catch (RuntimeException ignored) {
            // Ignore missing indexes from a first run or a previous successful cleanup.
        }
    }

    private void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }
}
