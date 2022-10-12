package org.springframework.data.aerospike.core;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.query.model.Index;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.springframework.data.aerospike.AwaitilityUtils.awaitTenSecondsUntil;

public class AerospikeTemplateIndexTests extends BaseBlockingIntegrationTests {

    private static final String INDEX_TEST_1 = "index-test-77777";
    private static final String INDEX_TEST_2 = "index-test-88888";

    @Override
    @BeforeEach
    public void setUp() {
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedDocument.class, INDEX_TEST_1);
        additionalAerospikeTestOperations.dropIndexIfExists(IndexedDocument.class, INDEX_TEST_2);
    }

    @Test
    public void createIndex_createsIndexIfExecutedConcurrently() {
        AtomicInteger errors = new AtomicInteger();
        AsyncUtils.executeConcurrently(5, () -> {
            try {
                template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);
            } catch (IndexAlreadyExistsException e) {
                errors.incrementAndGet();
            }
        });

        awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());
        assertThat(errors.get()).isLessThanOrEqualTo(4);// depending on the timing all 5 requests can succeed on Aerospike Server
    }

    @Test
    public void createIndex_allCreateIndexConcurrentAttemptsShouldNotFailIfIndexAlreadyExists() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());

        AtomicInteger errors = new AtomicInteger();
        AsyncUtils.executeConcurrently(5, () -> {
            try {
                template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);
            } catch (IndexAlreadyExistsException e) {
                errors.incrementAndGet();
            }
        });

        assertThat(errors.get()).isEqualTo(0);
    }

    @Test
    public void createIndex_createsIndex() {
        String setName = template.getSetName(IndexedDocument.class);
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                        .contains(Index.builder().name(INDEX_TEST_1).namespace(namespace).set(setName).bin("stringField")
                                .indexType(IndexType.STRING).build())
        );
    }

    @Test
    public void createIndex_shouldNoThrowExceptionIfIndexAlreadyExists() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() -> assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());

        assertThatCode(() -> template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING))
                .doesNotThrowAnyException();
    }

    @Test
    public void createIndex_createsListIndex() {
        String setName = template.getSetName(IndexedDocument.class);
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "listField", IndexType.STRING, IndexCollectionType.LIST);

        awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                        .contains(Index.builder().name(INDEX_TEST_1).namespace(namespace).set(setName).bin("listField")
                                .indexType(IndexType.STRING).indexCollectionType(IndexCollectionType.LIST).build())
        );
    }

    @Test
    public void createIndex_createsMapIndex() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapField", IndexType.STRING, IndexCollectionType.MAPKEYS);
        template.createIndex(IndexedDocument.class, INDEX_TEST_2, "mapField", IndexType.STRING, IndexCollectionType.MAPVALUES);

        awaitTenSecondsUntil(() -> {
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue();
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_2)).isTrue();
        });
    }

    @Test
    public void createIndex_createsIndexForDifferentTypes() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapField", IndexType.STRING);
        template.createIndex(IndexedDocument.class, INDEX_TEST_2, "mapField", IndexType.NUMERIC);

        awaitTenSecondsUntil(() -> {
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue();
            assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_2)).isTrue();
        });
    }

    @Test
    public void deleteIndex_doesNotThrowExceptionIfIndexDoesNotExist() {
        assertThatCode(() -> template.deleteIndex(IndexedDocument.class, "not-existing-index"))
                .doesNotThrowAnyException();
    }

    @Test
    public void createIndex_createsIndexOnNestedList() {
        String setName = template.getSetName(IndexedDocument.class);
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "nestedList", IndexType.STRING, IndexCollectionType.LIST, CTX.listIndex(1));

        awaitTenSecondsUntil(() -> {
                    CTX ctx = Objects.requireNonNull(additionalAerospikeTestOperations.getIndexes(setName).stream()
                            .filter(o -> o.getName().equals(INDEX_TEST_1))
                            .findFirst().orElse(null)).getCTX()[0];

                    assertThat(ctx.id).isEqualTo(CTX.listIndex(1).id);
                    assertThat(ctx.value.toLong()).isEqualTo(CTX.listIndex(1).value.toLong());
                }
        );
    }

    @Test
    public void createIndex_createsIndexOnNestedListContextRank() {
        String setName = template.getSetName(IndexedDocument.class);
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "nestedList", IndexType.STRING, IndexCollectionType.LIST, CTX.listRank(-1));

        awaitTenSecondsUntil(() -> {
                    CTX ctx = Objects.requireNonNull(additionalAerospikeTestOperations.getIndexes(setName).stream()
                            .filter(o -> o.getName().equals(INDEX_TEST_1))
                            .findFirst().orElse(null)).getCTX()[0];

                    assertThat(ctx.id).isEqualTo(CTX.listRank(-1).id);
                    assertThat(ctx.value.toLong()).isEqualTo(CTX.listRank(-1).value.toLong());
                }
        );
    }

    @Test
    public void createIndex_createsIndexOnMapOfMapsContext() {
        String setName = template.getSetName(IndexedDocument.class);

        CTX[] ctx = new CTX[]{
                CTX.mapKey(com.aerospike.client.Value.get("key1")),
                CTX.mapKey(com.aerospike.client.Value.get("innerKey2"))
        };
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapOfLists", IndexType.STRING, IndexCollectionType.MAPKEYS, ctx);

        awaitTenSecondsUntil(() -> {
                    CTX[] ctxResponse = Objects.requireNonNull(additionalAerospikeTestOperations.getIndexes(setName).stream()
                            .filter(o -> o.getName().equals(INDEX_TEST_1))
                            .findFirst().orElse(null)).getCTX();

                    assertThat(ctx.length).isEqualTo(ctxResponse.length);
                    assertThat(ctx[0].id).isIn(ctxResponse[0].id, ctxResponse[1].id);
                    assertThat(ctx[1].id).isIn(ctxResponse[0].id, ctxResponse[1].id);
                    assertThat(ctx[0].value.toLong()).isIn(ctxResponse[0].value.toLong(), ctxResponse[1].value.toLong());
                    assertThat(ctx[1].value.toLong()).isIn(ctxResponse[0].value.toLong(), ctxResponse[1].value.toLong());
                }
        );
    }

    @Test
    public void deleteIndex_deletesExistingIndex() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

        awaitTenSecondsUntil(() -> assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isTrue());

        template.deleteIndex(IndexedDocument.class, INDEX_TEST_1);

        awaitTenSecondsUntil(() -> assertThat(additionalAerospikeTestOperations.indexExists(INDEX_TEST_1)).isFalse());
    }

    @Test
    void indexedAnnotation_createsIndexes() {
        AutoIndexedDocumentAssert.assertIndexesCreated(additionalAerospikeTestOperations, namespace);
    }

    @Value
    @Document
    public static class IndexedDocument {
        String stringField;
        int intField;
        List<List<String>> nestedList;
        Map<String, Map<String, String>> mapOfMaps;
    }
}
