package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.IndexNotFoundException;
import org.springframework.data.aerospike.mapping.Document;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReactiveAerospikeTemplateIndexTests extends BaseReactiveIntegrationTests {

    private static final String INDEX_TEST_1 = "index-test-77777";

    @Override
    @BeforeEach
    public void setUp() {
        blockingAerospikeTestOperations.dropIndexIfExists(IndexedDocument.class, INDEX_TEST_1);
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
