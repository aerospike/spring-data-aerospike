package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.exceptions.IndexNotFoundException;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.query.model.Index;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.util.AwaitilityUtils.awaitTenSecondsUntil;

@Extensive
public class ReactiveAerospikeTemplateIndexTests extends BaseReactiveIntegrationTests {

    private static final String INDEX_TEST_1 = "index-test-77777";
    private static final String INDEX_TEST_2 = "index-test-88888";

    @Override
    @BeforeEach
    public void setUp() {
        additionalAerospikeTestOperations.dropIndex(IndexedDocument.class, INDEX_TEST_1);
        additionalAerospikeTestOperations.dropIndex(IndexedDocument.class, INDEX_TEST_2);
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void createIndex_shouldNotThrowExceptionIfIndexAlreadyExists() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING).block();

            assertThatCode(() -> reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField",
                    IndexType.STRING)
                .block())
                .doesNotThrowAnyException();
        }
    }

    @Test
    public void createIndex_createsIndexIfExecutedConcurrently() {
        AtomicInteger errorsCount = new AtomicInteger();

        IntStream.range(0, 5)
            .mapToObj(i -> reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField",
                    IndexType.STRING)
                .onErrorResume(throwable -> {
                    errorsCount.incrementAndGet();
                    return Mono.empty();
                }))
            .forEach(Mono::block);

        assertThat(errorsCount.get()).isLessThanOrEqualTo(4); // depending on the timing
        assertThat(reactiveTemplate.indexExists(INDEX_TEST_1).toFuture().getNow(false)).isTrue();
    }

    @Test
    public void createIndex_createsIndex() {
        String setName = reactiveTemplate.getSetName(IndexedDocument.class);
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField",
                IndexType.STRING)
            .block();

        awaitTenSecondsUntil(() ->
            assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                .contains(Index.builder().name(INDEX_TEST_1).namespace(namespace).set(setName).bin("stringField")
                    .indexType(IndexType.STRING).build())
        );
    }

    @Test
    public void createIndex_createsListIndex() {
        String setName = reactiveTemplate.getSetName(IndexedDocument.class);
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "listField",
                IndexType.STRING, IndexCollectionType.LIST)
            .block();

        awaitTenSecondsUntil(() ->
            assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                .contains(Index.builder().name(INDEX_TEST_1).namespace(namespace).set(setName).bin("listField")
                    .indexType(IndexType.STRING).indexCollectionType(IndexCollectionType.LIST).build())
        );
    }

    @Test
    public void createIndex_createsMapIndex() {
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapField",
                IndexType.STRING, IndexCollectionType.MAPKEYS)
            .block();
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_2, "mapField",
                IndexType.STRING, IndexCollectionType.MAPVALUES)
            .block();

        awaitTenSecondsUntil(() -> {
            assertThat(reactiveTemplate.indexExists(INDEX_TEST_1).toFuture().getNow(false)).isTrue();
            assertThat(reactiveTemplate.indexExists(INDEX_TEST_2).toFuture().getNow(false)).isTrue();
        });
    }

    @Test
    public void createIndex_createsIndexForDifferentTypes() {
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapField",
                IndexType.STRING)
            .block();
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_2, "mapField",
                IndexType.NUMERIC)
            .block();

        awaitTenSecondsUntil(() -> {
            assertThat(reactiveTemplate.indexExists(INDEX_TEST_1).toFuture().getNow(false)).isTrue();
            assertThat(reactiveTemplate.indexExists(INDEX_TEST_2).toFuture().getNow(false)).isTrue();
        });
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void createIndex_createsIndexOnNestedList() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            String setName = reactiveTemplate.getSetName(IndexedDocument.class);
            reactiveTemplate.createIndex(
                IndexedDocument.class, INDEX_TEST_1, "nestedList",
                IndexType.STRING, IndexCollectionType.LIST, CTX.listIndex(1)).block();

            awaitTenSecondsUntil(() -> {
                    CTX ctx = Objects.requireNonNull(additionalAerospikeTestOperations.getIndexes(setName).stream()
                        .filter(o -> o.getName().equals(INDEX_TEST_1))
                        .findFirst().orElse(null)).getCtx()[0];

                    assertThat(ctx.id).isEqualTo(CTX.listIndex(1).id);
                    assertThat(ctx.value.toLong()).isEqualTo(CTX.listIndex(1).value.toLong());
                }
            );
        }
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void createIndex_createsIndexOnMapOfMapsContext() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            String setName = reactiveTemplate.getSetName(IndexedDocument.class);

            CTX[] ctx = new CTX[]{
                CTX.mapKey(com.aerospike.client.Value.get("key1")),
                CTX.mapKey(com.aerospike.client.Value.get("innerKey2"))
            };
            reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1,
                "mapOfLists", IndexType.STRING, IndexCollectionType.MAPKEYS, ctx).block();

            awaitTenSecondsUntil(() -> {
                    CTX[] ctxResponse =
                        Objects.requireNonNull(additionalAerospikeTestOperations.getIndexes(setName).stream()
                            .filter(o -> o.getName().equals(INDEX_TEST_1))
                            .findFirst().orElse(null)).getCtx();

                    assertThat(ctx.length).isEqualTo(ctxResponse.length);
                    assertThat(ctx[0].id).isIn(ctxResponse[0].id, ctxResponse[1].id);
                    assertThat(ctx[1].id).isIn(ctxResponse[0].id, ctxResponse[1].id);
                    assertThat(ctx[0].value.toLong()).isIn(ctxResponse[0].value.toLong(),
                        ctxResponse[1].value.toLong());
                    assertThat(ctx[1].value.toLong()).isIn(ctxResponse[0].value.toLong(),
                        ctxResponse[1].value.toLong());
                }
            );
        }
    }

    @Test
    public void createIndexWithExpression_createsIndex() {
        if (serverVersionSupport.isServerVersionGtOrEq8_1()) {
            // Build an expression that indexes age only for adults with expected names that are 18 or older
            Expression filterExp = Exp.build(
                Exp.cond(
                    Exp.and(
                        Exp.ge(// Is the age 18 or older?
                            Exp.intBin("age"),
                            Exp.val(18)
                        ),
                        ListExp.getByValue( // Do they have the expected names?
                            ListReturnType.EXISTS,
                            Exp.stringBin("firstName"),
                            Exp.val(List.of("Jane", "Tricia", "Peter"))
                        )
                    ),
                    Exp.intBin("age"), // If true, return the age of the customer to be indexed
                    Exp.unknown() // returns "unknown" to exclude the record from the index
                )
            );
            String setName = reactiveTemplate.getSetName(IndexedDocument.class);
            String indexName = "idx_selected_names_above_18";
            reactiveTemplate.createIndex(setName, indexName, IndexType.NUMERIC, IndexCollectionType.DEFAULT, filterExp)
                .block();
            assertThat(reactiveTemplate.indexExists(indexName).block()).isTrue();

            awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                    .contains(Index.builder().name(indexName).namespace(namespace).set(setName)
                        .bin("null")
                        .indexType(IndexType.NUMERIC).build())
            );

            // Cleanup
            reactiveTemplate.deleteIndex(setName, indexName);
        }
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void deleteIndex_doesNotThrowExceptionIfIndexDoesNotExist() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            assertThatCode(() -> reactiveTemplate.deleteIndex(IndexedDocument.class, "not-existing-index")
                .block())
                .doesNotThrowAnyException();
        }
    }

    // for Aerospike Server ver. < 6.1.0.1
    @Test
    public void deleteIndex_throwsExceptionIfIndexDoesNotExist() {
        if (!serverVersionSupport.isDropCreateBehaviorUpdated()) {
            assertThatThrownBy(() -> reactiveTemplate.deleteIndex(IndexedDocument.class, "not-existing-index").block())
                .isInstanceOf(IndexNotFoundException.class);
        }
    }

    @Test
    public void deleteIndex_deletesExistingIndex() {
        reactiveTemplate.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING).block();
        reactiveTemplate.deleteIndex(IndexedDocument.class, INDEX_TEST_1).block();
        assertThat(reactiveTemplate.indexExists(INDEX_TEST_1).toFuture().getNow(false)).isFalse();
    }

    @Value
    @Document
    public static class IndexedDocument {

        String stringField;
        int intField;
    }
}
