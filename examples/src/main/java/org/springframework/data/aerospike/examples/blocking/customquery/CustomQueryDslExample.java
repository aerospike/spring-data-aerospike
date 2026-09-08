package org.springframework.data.aerospike.examples.blocking.customquery;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.StreamSupport;

@Component
public class CustomQueryDslExample {

    private final CustomQueryMovieRepository repository;
    private final AerospikeTemplate template;

    public CustomQueryDslExample(CustomQueryMovieRepository repository, AerospikeTemplate template) {
        this.repository = repository;
        this.template = template;
    }

    public void run() {
        resetExampleData();
        template.createIndex(CustomQueryMovieDocument.class, CustomQueryMovieDocument.RELEASE_YEAR_INDEX,
            "releaseYear", IndexType.NUMERIC);

        repository.saveAll(List.of(
            new CustomQueryMovieDocument("custom-query-1", "Stalker", "science-fiction", 1979),
            new CustomQueryMovieDocument("custom-query-2", "Children of Men", "science-fiction", 2006),
            new CustomQueryMovieDocument("custom-query-3", "High and Low", "crime", 1963)
        ));

        Query query = new Query(
            Qualifier.dslExpressionBuilder()
                .setDSLExpressionString("$.releaseYear >= ?0 and $.releaseYear < ?1")
                .setDSLExpressionIndexToUse(CustomQueryMovieDocument.RELEASE_YEAR_INDEX)
                .setDSLExpressionValues(new Object[]{2000, 2010})
                .build()
        );
        List<CustomQueryMovieDocument> modernMovies = StreamSupport
            .stream(repository.findUsingQuery(query).spliterator(), false)
            .toList();
        require(modernMovies.size() == 1, "Expected one movie from the custom DSL query");
        require("Children of Men".equals(modernMovies.get(0).getTitle()), "DSL query result title did not match");

        System.out.println("Created a numeric index and queried it with a custom DSL qualifier.");
    }

    private void resetExampleData() {
        template.deleteAll(CustomQueryMovieDocument.class);
        try {
            template.deleteIndex(CustomQueryMovieDocument.class, CustomQueryMovieDocument.RELEASE_YEAR_INDEX);
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
