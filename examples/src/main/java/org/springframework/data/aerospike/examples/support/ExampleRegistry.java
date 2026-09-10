package org.springframework.data.aerospike.examples.support;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.examples.blocking.converters.BlockingCustomConvertersExample;
import org.springframework.data.aerospike.examples.blocking.converters.config.CustomConvertersAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.converters.entity.ConverterOrderDocument;
import org.springframework.data.aerospike.examples.blocking.crud.BlockingRepositoryCrudExample;
import org.springframework.data.aerospike.examples.blocking.crud.config.AerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.crud.entity.MovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.CustomQueryDslExample;
import org.springframework.data.aerospike.examples.blocking.customquery.config.CustomQueryConfiguration;
import org.springframework.data.aerospike.examples.blocking.customquery.entity.CustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.ProgrammaticCustomQueryExample;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.config.ProgrammaticCustomQueryConfiguration;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.entity.ProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.indexed.IndexedAnnotationExample;
import org.springframework.data.aerospike.examples.blocking.indexed.config.IndexedAnnotationConfiguration;
import org.springframework.data.aerospike.examples.blocking.indexed.entity.AnnotatedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.projection.ProjectionExample;
import org.springframework.data.aerospike.examples.blocking.projection.config.ProjectionAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.projection.entity.ProjectedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.query.SecondaryIndexQueryExample;
import org.springframework.data.aerospike.examples.blocking.query.config.QueryAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.query.entity.IndexedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.querymethods.BlockingRepositoryQueryMethodsExample;
import org.springframework.data.aerospike.examples.blocking.querymethods.config.QueryMethodsAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.querymethods.entity.QueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.blocking.template.BlockingTemplateExample;
import org.springframework.data.aerospike.examples.blocking.template.config.TemplateAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.template.entity.TemplateMovieDocument;
import org.springframework.data.aerospike.examples.blocking.transactions.BlockingTransactionExample;
import org.springframework.data.aerospike.examples.blocking.transactions.config.BlockingTransactionAerospikeConfiguration;
import org.springframework.data.aerospike.examples.blocking.transactions.entity.BlockingTransactionalMovieDocument;
import org.springframework.data.aerospike.examples.reactive.converters.ReactiveCustomConvertersExample;
import org.springframework.data.aerospike.examples.reactive.converters.config.ReactiveCustomConvertersAerospikeConfiguration;
import org.springframework.data.aerospike.examples.reactive.converters.entity.ReactiveConverterOrderDocument;
import org.springframework.data.aerospike.examples.reactive.customquery.ReactiveProgrammaticCustomQueryExample;
import org.springframework.data.aerospike.examples.reactive.customquery.config.ReactiveProgrammaticCustomQueryConfiguration;
import org.springframework.data.aerospike.examples.reactive.customquery.entity.ReactiveProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.reactive.crud.ReactiveRepositoryCrudExample;
import org.springframework.data.aerospike.examples.reactive.crud.config.ReactiveAerospikeConfiguration;
import org.springframework.data.aerospike.examples.reactive.crud.entity.ReactiveMovieDocument;
import org.springframework.data.aerospike.examples.reactive.querymethods.ReactiveRepositoryQueryMethodsExample;
import org.springframework.data.aerospike.examples.reactive.querymethods.config.ReactiveQueryMethodsAerospikeConfiguration;
import org.springframework.data.aerospike.examples.reactive.querymethods.entity.ReactiveQueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.reactive.template.ReactiveTemplateExample;
import org.springframework.data.aerospike.examples.reactive.template.config.ReactiveTemplateAerospikeConfiguration;
import org.springframework.data.aerospike.examples.reactive.template.entity.ReactiveTemplateMovieDocument;
import org.springframework.data.aerospike.examples.reactive.transactions.ReactiveTransactionExample;
import org.springframework.data.aerospike.examples.reactive.transactions.config.ReactiveTransactionAerospikeConfiguration;
import org.springframework.data.aerospike.examples.reactive.transactions.entity.ReactiveTransactionalMovieDocument;

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
            ),
            ExampleDefinition.of(
                "blocking-query-methods",
                "blocking",
                QueryMethodsAerospikeConfiguration.class,
                BlockingRepositoryQueryMethodsExample.class,
                ExampleFixture.cleanSetAndCreateIndexesBeforeContextRefresh(QueryMethodsMovieDocument.class,
                    ExampleFixture.index(QueryMethodsMovieDocument.GENRE_INDEX, "genre", IndexType.STRING),
                    ExampleFixture.index(QueryMethodsMovieDocument.RELEASE_YEAR_INDEX, "releaseYear", IndexType.NUMERIC)),
                "repository", "query", "derived"
            ),
            ExampleDefinition.of(
                "reactive-query-methods",
                "reactive",
                ReactiveQueryMethodsAerospikeConfiguration.class,
                ReactiveRepositoryQueryMethodsExample.class,
                ExampleFixture.cleanSetAndCreateIndexesBeforeContextRefresh(ReactiveQueryMethodsMovieDocument.class,
                    ExampleFixture.index(ReactiveQueryMethodsMovieDocument.GENRE_INDEX, "genre", IndexType.STRING),
                    ExampleFixture.index(ReactiveQueryMethodsMovieDocument.RELEASE_YEAR_INDEX, "releaseYear",
                        IndexType.NUMERIC)),
                "reactive", "repository", "query", "derived"
            ),
            ExampleDefinition.of(
                "blocking-custom-query-programmatic",
                "blocking",
                ProgrammaticCustomQueryConfiguration.class,
                ProgrammaticCustomQueryExample.class,
                ExampleFixture.cleanSetAndCreateIndexesBeforeContextRefresh(ProgrammaticCustomQueryMovieDocument.class,
                    ExampleFixture.index(ProgrammaticCustomQueryMovieDocument.GENRE_INDEX, "genre", IndexType.STRING),
                    ExampleFixture.index(ProgrammaticCustomQueryMovieDocument.RELEASE_YEAR_INDEX, "releaseYear",
                        IndexType.NUMERIC)),
                "repository", "query", "custom", "programmatic"
            ),
            ExampleDefinition.of(
                "reactive-custom-query-programmatic",
                "reactive",
                ReactiveProgrammaticCustomQueryConfiguration.class,
                ReactiveProgrammaticCustomQueryExample.class,
                ExampleFixture.cleanSetAndCreateIndexesBeforeContextRefresh(
                    ReactiveProgrammaticCustomQueryMovieDocument.class,
                    ExampleFixture.index(ReactiveProgrammaticCustomQueryMovieDocument.GENRE_INDEX, "genre",
                        IndexType.STRING),
                    ExampleFixture.index(ReactiveProgrammaticCustomQueryMovieDocument.RELEASE_YEAR_INDEX, "releaseYear",
                        IndexType.NUMERIC)),
                "reactive", "repository", "query", "custom", "programmatic"
            ),
            ExampleDefinition.of(
                "blocking-template",
                "blocking",
                TemplateAerospikeConfiguration.class,
                BlockingTemplateExample.class,
                ExampleFixture.cleanSetAndCreateIndexesBeforeContextRefresh(TemplateMovieDocument.class,
                    ExampleFixture.index(TemplateMovieDocument.GENRE_INDEX, "genre", IndexType.STRING),
                    ExampleFixture.index(TemplateMovieDocument.RELEASE_YEAR_INDEX, "releaseYear", IndexType.NUMERIC)),
                "template", "query", "batch", "mutation"
            ),
            ExampleDefinition.of(
                "reactive-template",
                "reactive",
                ReactiveTemplateAerospikeConfiguration.class,
                ReactiveTemplateExample.class,
                ExampleFixture.cleanSetAndCreateIndexesBeforeContextRefresh(ReactiveTemplateMovieDocument.class,
                    ExampleFixture.index(ReactiveTemplateMovieDocument.GENRE_INDEX, "genre", IndexType.STRING),
                    ExampleFixture.index(ReactiveTemplateMovieDocument.RELEASE_YEAR_INDEX, "releaseYear",
                        IndexType.NUMERIC)),
                "reactive", "template", "query", "batch", "mutation"
            ),
            ExampleDefinition.of(
                "blocking-custom-converters",
                "blocking",
                CustomConvertersAerospikeConfiguration.class,
                BlockingCustomConvertersExample.class,
                ExampleFixture.cleanSet(ConverterOrderDocument.class),
                "template", "converter"
            ),
            ExampleDefinition.of(
                "reactive-custom-converters",
                "reactive",
                ReactiveCustomConvertersAerospikeConfiguration.class,
                ReactiveCustomConvertersExample.class,
                ExampleFixture.cleanSet(ReactiveConverterOrderDocument.class),
                "reactive", "template", "converter"
            ),
            ExampleDefinition.of(
                "blocking-transactions",
                "blocking",
                BlockingTransactionAerospikeConfiguration.class,
                BlockingTransactionExample.class,
                ExampleFixture.cleanSet(BlockingTransactionalMovieDocument.class),
                "repository", "template", "transaction"
            ),
            ExampleDefinition.of(
                "reactive-transactions",
                "reactive",
                ReactiveTransactionAerospikeConfiguration.class,
                ReactiveTransactionExample.class,
                ExampleFixture.cleanSet(ReactiveTransactionalMovieDocument.class),
                "reactive", "repository", "template", "transaction"
            )
        );
    }
}
