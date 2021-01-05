package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.IndexNotFoundException;
import org.springframework.data.aerospike.core.AerospikeTemplateIndexTests;
import org.springframework.data.aerospike.core.AutoIndexedDocumentAssert;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.sample.AutoIndexedDocument;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.AwaitilityUtils.awaitTenSecondsUntil;

public class ReactiveAerospikeTemplateIndexTests extends BaseReactiveIntegrationTests {

    private static final String INDEX_TEST_1 = "index-test-77777";
    private static final String INDEX_TEST_2 = "index-test-88888";

    @Override
    @BeforeEach
    public void setUp() {
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedDocument.class, INDEX_TEST_1);
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedDocument.class, INDEX_TEST_2);
    }

    @Test
    public void createIndex_throwsExceptionIfIndexAlreadyExists() {
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING).block();

        assertThatThrownBy(() -> reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING)
                .block()).isInstanceOf(IndexAlreadyExistsException.class);
    }

    @Test
    public void createIndex_createsIndexIfExecutedConcurrently() {
        AtomicInteger errorsCount = new AtomicInteger();

        IntStream.range(0, 5)
                .mapToObj(i -> reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING)
                        .onErrorResume(throwable -> {
                            errorsCount.incrementAndGet();
                            return Mono.empty();
                        }))
                .forEach(Mono::block);

        assertThat(errorsCount.get()).isLessThanOrEqualTo(4);// depending on the timing all 5 requests can succeed on Aerospike Server

        assertThat(indexExists(INDEX_TEST_1, "stringField")).isTrue();
    }

    @Test
    public void createIndex_createsIndex() {
        String setName = reactiveTemplate.getSetName(AerospikeTemplateIndexTests.IndexedDocument.class);
        reactiveTemplate.createIndex(AerospikeTemplateIndexTests.IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING).block();

        awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                        .contains(new Index(INDEX_TEST_1, namespace, setName, "stringField", IndexType.STRING, IndexCollectionType.DEFAULT))
        );
    }

    @Test
    public void createIndex_createsListIndex() {
        String setName = reactiveTemplate.getSetName(AerospikeTemplateIndexTests.IndexedDocument.class);
        reactiveTemplate.createIndex(AerospikeTemplateIndexTests.IndexedDocument.class, INDEX_TEST_1, "listField", IndexType.STRING, IndexCollectionType.LIST).block();

        awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                        .contains(new Index(INDEX_TEST_1, namespace, setName, "listField", IndexType.STRING, IndexCollectionType.LIST))
        );
    }

    @Test
    public void createIndex_createsMapIndex() {
        reactiveTemplate.createIndex(AerospikeTemplateIndexTests.IndexedDocument.class, INDEX_TEST_1, "mapField", IndexType.STRING, IndexCollectionType.MAPKEYS).block();
        reactiveTemplate.createIndex(AerospikeTemplateIndexTests.IndexedDocument.class, INDEX_TEST_2, "mapField", IndexType.STRING, IndexCollectionType.MAPVALUES).block();

        awaitTenSecondsUntil(() -> {
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue();
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_2)).isTrue();
        });
    }

    @Test
    public void createIndex_createsIndexForDifferentTypes() {
        reactiveTemplate.createIndex(AerospikeTemplateIndexTests.IndexedDocument.class, INDEX_TEST_1, "mapField", IndexType.STRING).block();
        reactiveTemplate.createIndex(AerospikeTemplateIndexTests.IndexedDocument.class, INDEX_TEST_2, "mapField", IndexType.NUMERIC).block();

        awaitTenSecondsUntil(() -> {
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue();
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_2)).isTrue();
        });
    }


    @Test
    public void deleteIndex_throwsExceptionIfIndexDoesNotExist() {
        assertThatThrownBy(() -> reactiveTemplate.deleteIndex(IndexedDocument.class, "not-existing-index").block())
                .isInstanceOf(IndexNotFoundException.class);
    }

    @Test
    public void deleteIndex_deletesExistingIndex() {
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING).block();

        reactiveTemplate.deleteIndex(IndexedDocument.class, INDEX_TEST_1).block();

        assertThat(indexExists(INDEX_TEST_1, "stringField")).isFalse();
    }

    @Test
    void indexedAnnotation_createsIndexes() {
        AutoIndexedDocumentAssert.assertIndexesCreated(additionalAerospikeTestOperations, namespace);
    }

    private boolean indexExists(String indexName, String binName) {
        try {
            reactiveTemplate.createIndex(IndexedDocument.class, indexName, binName, IndexType.STRING).block();
            reactiveTemplate.deleteIndex(IndexedDocument.class, indexName).block();
            return false;
        } catch (IndexAlreadyExistsException ex) {
            return true;
        }
    }

    @Value
    @Document
    public static class IndexedDocument {

        String stringField;
        int intField;
    }
}
