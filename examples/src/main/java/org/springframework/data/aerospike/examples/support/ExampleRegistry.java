package org.springframework.data.aerospike.examples.support;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.examples.blocking.crud.AerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.crud.BlockingRepositoryCrudExample;
import org.springframework.data.aerospike.examples.blocking.crud.MovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.CustomQueryConfiguration;
import org.springframework.data.aerospike.examples.blocking.customquery.CustomQueryDslExample;
import org.springframework.data.aerospike.examples.blocking.customquery.CustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.indexed.AnnotatedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.indexed.IndexedAnnotationConfiguration;
import org.springframework.data.aerospike.examples.blocking.indexed.IndexedAnnotationExample;
import org.springframework.data.aerospike.examples.blocking.projection.ProjectedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.projection.ProjectionAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.projection.ProjectionExample;
import org.springframework.data.aerospike.examples.blocking.query.IndexedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.query.QueryAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.query.SecondaryIndexQueryExample;
import org.springframework.data.aerospike.examples.reactive.crud.ReactiveAerospikeConfiguration;
import org.springframework.data.aerospike.examples.reactive.crud.ReactiveMovieDocument;
import org.springframework.data.aerospike.examples.reactive.crud.ReactiveRepositoryCrudExample;

import java.util.List;

public final class ExampleRegistry {

    private ExampleRegistry() {
    }

    public static List<ExampleDefinition> all() {
        return List.of(
            ExampleDefinition.of(
                "blocking-crud",
                "blocking",
                AerospikeConfiguration.class,
                BlockingRepositoryCrudExample.class,
                ExampleFixture.cleanSet(MovieDocument.class),
                "repository", "crud"
            ),
            ExampleDefinition.of(
                "reactive-crud",
                "reactive",
                ReactiveAerospikeConfiguration.class,
                ReactiveRepositoryCrudExample.class,
                ExampleFixture.cleanSet(ReactiveMovieDocument.class),
                "reactive", "repository", "crud"
            ),
            ExampleDefinition.of(
                "indexed-query",
                "blocking",
                QueryAerospikeConfiguration.class,
                SecondaryIndexQueryExample.class,
                ExampleFixture.cleanSetAndIndexes(IndexedMovieDocument.class, IndexedMovieDocument.GENRE_INDEX),
                "repository", "query", "secondary-index"
            ),
            ExampleDefinition.of(
                "projection",
                "blocking",
                ProjectionAerospikeConfiguration.class,
                ProjectionExample.class,
                ExampleFixture.cleanSet(ProjectedMovieDocument.class),
                "repository", "projection"
            ),
            ExampleDefinition.of(
                "indexed-annotation",
                "blocking",
                IndexedAnnotationConfiguration.class,
                IndexedAnnotationExample.class,
                ExampleFixture.cleanSetAndDropIndexesBeforeContextRefresh(AnnotatedMovieDocument.class,
                    AnnotatedMovieDocument.GENRE_INDEX),
                "indexed", "repository", "startup-index"
            ),
            ExampleDefinition.of(
                "custom-query-dsl",
                "blocking",
                CustomQueryConfiguration.class,
                CustomQueryDslExample.class,
                ExampleFixture.cleanSetAndCreateIndexBeforeContextRefresh(CustomQueryMovieDocument.class,
                    CustomQueryMovieDocument.RELEASE_YEAR_INDEX, "releaseYear", IndexType.NUMERIC),
                "repository", "query", "dsl"
            )
        );
    }
}
