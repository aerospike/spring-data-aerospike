package org.springframework.data.aerospike.examples.blocking.customquery.types;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.RegexFlag;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.customquery.types.entity.CustomQueryTypesMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.types.repository.CustomQueryTypesMovieRepository;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;

import java.util.Comparator;
import java.util.List;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;
import static org.springframework.data.aerospike.examples.support.ExampleCollections.toSortedList;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata.SINCE_UPDATE_TIME;

// Demonstrates the custom query qualifier types supported by repository queries.
public class CustomQueryTypesExample {

    private final CustomQueryTypesMovieRepository repository;
    private final AerospikeTemplate template;
    private final ServerVersionSupport serverVersionSupport;

    public CustomQueryTypesExample(CustomQueryTypesMovieRepository repository, AerospikeTemplate template,
                                   ServerVersionSupport serverVersionSupport) {
        this.repository = repository;
        this.template = template;
        this.serverVersionSupport = serverVersionSupport;
    }

    public void run() {
        repository.saveAll(seedMovies());

        regularQualifier();
        idQualifier();
        idAndRegularQualifier();
        metadataQualifier();
        staticDslExpressionQualifier();
        dslExpressionQualifier();
        complexDslExpressionQualifier();
        filteringQualifier();
        indexedExpressionQualifier();

        System.out.println("Ran custom query qualifier type examples");
    }

    private void regularQualifier() {
        // tag::custom-query-regular-qualifier[]
        // A regular qualifier maps one document property to a comparison operation.
        Qualifier scienceFiction = Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue("science-fiction")
            .build();

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(new Query(scienceFiction));
        // end::custom-query-regular-qualifier[]

        requireTitles(result, "Expected regular qualifier results", "Children of Men", "Solaris", "Stalker");
    }

    private void idQualifier() {
        // tag::custom-query-id-qualifier[]
        // An id qualifier targets the Aerospike user key instead of a document bin.
        Qualifier keyEquals = Qualifier.idEquals("custom-query-types-3");

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(new Query(keyEquals));
        // end::custom-query-id-qualifier[]

        requireTitles(result, "Expected id qualifier result", "High and Low");
    }

    private void idAndRegularQualifier() {
        // tag::custom-query-id-and-regular-qualifier[]
        // An id qualifier can be combined with a bin qualifier to narrow by key and record content.
        Query query = new Query(Qualifier.and(
            Qualifier.idEquals("custom-query-types-1"),
            genreQualifier("science-fiction")));

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(query);
        // end::custom-query-id-and-regular-qualifier[]

        requireTitles(result, "Expected id and regular qualifier result", "Stalker");
    }

    private void metadataQualifier() {
        // tag::custom-query-metadata-qualifier[]
        // Metadata qualifiers compare Aerospike record metadata such as last update time.
        Qualifier recentlyUpdated = Qualifier.metadataBuilder()
            .setMetadataField(SINCE_UPDATE_TIME)
            .setFilterOperation(FilterOperation.LT)
            .setValue(60_000L)
            .build();

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(new Query(recentlyUpdated));
        // end::custom-query-metadata-qualifier[]

        require(toSortedList(result, Comparator.comparing(CustomQueryTypesMovieDocument::getId)).size() == 5,
            "Expected metadata qualifier to match recently written records");
    }

    private void dslExpressionQualifier() {
        // tag::custom-query-dsl-expression-qualifier[]
        // DSL expression qualifiers bind method-style values into a server-side filter expression.
        Query query = new Query(Qualifier.dslExpressionBuilder()
            .setDSLExpressionString("$.genre == ?0 and $.releaseYear >= ?1")
            .setDSLExpressionValues(new Object[]{"science-fiction", 2000})
            .build());

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(query);
        // end::custom-query-dsl-expression-qualifier[]

        requireTitles(result, "Expected DSL expression qualifier result", "Children of Men");
    }

    private void complexDslExpressionQualifier() {
        // tag::custom-query-complex-dsl-expression-qualifier[]
        // Complex DSL expressions can group AND/OR logic that is awkward to express with plain qualifiers.
        Query query = new Query(Qualifier.dslExpressionBuilder()
            .setDSLExpressionString(
                "($.genre == 'science-fiction' or $.director == 'Francis Ford Coppola') and $.releaseYear >= 1970")
            .build());

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(query);
        // end::custom-query-complex-dsl-expression-qualifier[]

        requireTitles(result, "Expected complex DSL expression qualifier result",
            "Children of Men", "Solaris", "Stalker", "The Conversation");
    }

    private void staticDslExpressionQualifier() {
        // tag::custom-query-static-dsl-expression-qualifier[]
        // Static DSL expressions keep all predicate values inside the expression string.
        Qualifier seventiesScienceFiction = Qualifier.dslExpressionBuilder()
            .setDSLExpressionString("$.genre == 'science-fiction' and $.releaseYear < 1980")
            .build();

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(
            new Query(seventiesScienceFiction));
        // end::custom-query-static-dsl-expression-qualifier[]

        requireTitles(result, "Expected static DSL expression qualifier results", "Solaris", "Stalker");
    }

    private void filteringQualifier() {
        // tag::custom-query-filtering-qualifier[]
        // A filtering qualifier combines a secondary-index filter with an Aerospike expression.
        Expression directorContainsFord = Exp.build(Exp.regexCompare(
            ".*ford.*",
            RegexFlag.ICASE,
            Exp.stringBin("director")
        ));
        Filter releasedAfter1969 = Filter.range("releaseYear", 1970, Long.MAX_VALUE);

        Qualifier filterQualifier = Qualifier.filterBuilder()
            .setExpression(directorContainsFord)
            .setFilter(releasedAfter1969)
            .build();

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(new Query(filterQualifier));
        // end::custom-query-filtering-qualifier[]

        requireTitles(result, "Expected filtering qualifier result", "The Conversation");
    }

    private void indexedExpressionQualifier() {
        if (!serverVersionSupport.isServerVersionGtOrEq8_1()) {
            return;
        }

        String setName = template.getSetName(CustomQueryTypesMovieDocument.class);
        deleteExpressionIndexIfPresent(setName);

        // tag::custom-query-indexed-expression-qualifier[]
        // Expression indexes must exist before the qualifier can use them as the statement filter.
        Expression releaseYearForScienceFiction = Exp.build(Exp.cond(
            Exp.eq(Exp.stringBin("genre"), Exp.val("science-fiction")),
            Exp.intBin("releaseYear"),
            Exp.unknown()
        ));

        template.createIndex(setName, CustomQueryTypesMovieDocument.EXPRESSION_INDEX, IndexType.NUMERIC,
            IndexCollectionType.DEFAULT, releaseYearForScienceFiction);

        Qualifier usingExpressionIndex = Qualifier.indexedWithExpressionBuilder()
            .setIndexName(CustomQueryTypesMovieDocument.EXPRESSION_INDEX)
            .setFilterOperation(FilterOperation.BETWEEN)
            .setValue(1970)
            .setSecondValue(1980)
            .build();

        Iterable<CustomQueryTypesMovieDocument> result = repository.findUsingQuery(new Query(usingExpressionIndex));
        // end::custom-query-indexed-expression-qualifier[]

        try {
            requireTitles(result, "Expected indexed expression qualifier results", "Solaris", "Stalker");
        } finally {
            // The example owns this server-side expression index and removes it after the query.
            deleteExpressionIndexIfPresent(setName);
        }
    }

    private List<CustomQueryTypesMovieDocument> seedMovies() {
        return List.of(
            new CustomQueryTypesMovieDocument("custom-query-types-1", "Stalker", "science-fiction",
                "Andrei Tarkovsky", 1979),
            new CustomQueryTypesMovieDocument("custom-query-types-2", "Solaris", "science-fiction",
                "Andrei Tarkovsky", 1972),
            new CustomQueryTypesMovieDocument("custom-query-types-3", "High and Low", "crime",
                "Akira Kurosawa", 1963),
            new CustomQueryTypesMovieDocument("custom-query-types-4", "Children of Men", "science-fiction",
                "Alfonso Cuaron", 2006),
            new CustomQueryTypesMovieDocument("custom-query-types-5", "The Conversation", "thriller",
                "Francis Ford Coppola", 1974)
        );
    }

    private void requireTitles(Iterable<CustomQueryTypesMovieDocument> movies, String message,
                               String... expectedTitles) {
        List<String> titles = toSortedList(movies, Comparator.comparing(CustomQueryTypesMovieDocument::getTitle))
            .stream()
            .map(CustomQueryTypesMovieDocument::getTitle)
            .toList();
        require(titles.equals(List.of(expectedTitles)), message + ": " + titles);
    }

    private Qualifier genreQualifier(String genre) {
        return Qualifier.builder()
            .setPath("genre")
            .setFilterOperation(FilterOperation.EQ)
            .setValue(genre)
            .build();
    }

    private void deleteExpressionIndexIfPresent(String setName) {
        try {
            template.deleteIndex(setName, CustomQueryTypesMovieDocument.EXPRESSION_INDEX);
        } catch (RuntimeException ignored) {
            // Cleanup should not fail because the index may not exist on a fresh run.
        }
    }
}
