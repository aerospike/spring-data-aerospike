package org.springframework.data.aerospike.core.blocking;

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
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.annotation.Extensive;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.springframework.data.aerospike.util.AwaitilityUtils.awaitTenSecondsUntil;

@Extensive
@TestPropertySource(properties = {"spring.data.aerospike.create-indexes-on-startup=true"})
// this test class requires secondary indexes created on startup
public class AerospikeTemplateIndexTests extends BaseBlockingIntegrationTests {

    public static final String SET_NAME = "setAerospikeTemplateIndexTests";

    private static final String INDEX_TEST_1 = "index-test-77777";
    private static final String INDEX_TEST_1_WITH_SET = "index-test-77777" + SET_NAME;
    private static final String INDEX_TEST_2 = "index-test-88888";
    private static final String INDEX_TEST_2_WITH_SET = "index-test-88888" + SET_NAME;

    @Override
    @BeforeEach
    public void setUp() {
        additionalAerospikeTestOperations.dropIndex(IndexedDocument.class, INDEX_TEST_1);
        additionalAerospikeTestOperations.dropIndex(IndexedDocument.class, INDEX_TEST_2);
        additionalAerospikeTestOperations.dropIndex(SET_NAME, INDEX_TEST_1_WITH_SET);
        additionalAerospikeTestOperations.dropIndex(SET_NAME, INDEX_TEST_2_WITH_SET);
    }

    @Test
    public void createIndex_createsIndex() {
        String setName = template.getSetName(IndexedDocument.class);
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);
        assertThat(template.indexExists(INDEX_TEST_1)).isTrue();

        awaitTenSecondsUntil(() ->
            assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                .contains(Index.builder().name(INDEX_TEST_1).namespace(namespace).set(setName).bin("stringField")
                    .indexType(IndexType.STRING).build())
        );
    }

    @Test
    public void createIndexWithSetName_createsIndex() {
        template.createIndex(SET_NAME, INDEX_TEST_1_WITH_SET, "stringField", IndexType.STRING);
        assertThat(template.indexExists(INDEX_TEST_1_WITH_SET)).isTrue();

        awaitTenSecondsUntil(() ->
            assertThat(additionalAerospikeTestOperations.getIndexes(SET_NAME))
                .contains(Index.builder().name(INDEX_TEST_1_WITH_SET).namespace(namespace).set(SET_NAME)
                    .bin("stringField")
                    .indexType(IndexType.STRING).build())
        );
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void createIndex_shouldNotThrowExceptionIfIndexAlreadyExists() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);

            awaitTenSecondsUntil(() -> assertThat(template.indexExists(INDEX_TEST_1)).isTrue());

            assertThatCode(() -> template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField",
                IndexType.STRING)).doesNotThrowAnyException();
        }
    }

    @Test
    public void createIndex_createsListIndex() {
        String setName = template.getSetName(IndexedDocument.class);
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "listField", IndexType.STRING,
            IndexCollectionType.LIST);

        awaitTenSecondsUntil(() ->
            assertThat(additionalAerospikeTestOperations.getIndexes(setName))
                .contains(Index.builder().name(INDEX_TEST_1).namespace(namespace).set(setName).bin("listField")
                    .indexType(IndexType.STRING).indexCollectionType(IndexCollectionType.LIST).build())
        );
    }

    @Test
    public void createIndexWithSetName_createsListIndex() {
        template.createIndex(SET_NAME, INDEX_TEST_1_WITH_SET, "listField", IndexType.STRING,
            IndexCollectionType.LIST);

        awaitTenSecondsUntil(() ->
            assertThat(additionalAerospikeTestOperations.getIndexes(SET_NAME))
                .contains(Index.builder().name(INDEX_TEST_1_WITH_SET).namespace(namespace).set(SET_NAME)
                    .bin("listField")
                    .indexType(IndexType.STRING).indexCollectionType(IndexCollectionType.LIST).build())
        );
    }

    @Test
    public void createIndex_createsMapIndex() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapField", IndexType.STRING,
            IndexCollectionType.MAPKEYS);
        template.createIndex(IndexedDocument.class, INDEX_TEST_2, "mapField", IndexType.STRING,
            IndexCollectionType.MAPVALUES);

        awaitTenSecondsUntil(() -> {
            assertThat(template.indexExists(INDEX_TEST_1)).isTrue();
            assertThat(template.indexExists(INDEX_TEST_2)).isTrue();
        });
    }

    @Test
    public void createIndexWithSetName_createsMapIndex() {
        template.createIndex(SET_NAME, INDEX_TEST_1_WITH_SET, "mapField", IndexType.STRING,
            IndexCollectionType.MAPKEYS);
        template.createIndex(SET_NAME, INDEX_TEST_2_WITH_SET, "mapField", IndexType.STRING,
            IndexCollectionType.MAPVALUES);

        awaitTenSecondsUntil(() -> {
            assertThat(template.indexExists(INDEX_TEST_1_WITH_SET)).isTrue();
            assertThat(template.indexExists(INDEX_TEST_2_WITH_SET)).isTrue();
        });
    }

    @Test
    public void createIndex_createsIndexForDifferentTypes() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapField", IndexType.STRING);
        template.createIndex(IndexedDocument.class, INDEX_TEST_2, "mapField", IndexType.NUMERIC);

        awaitTenSecondsUntil(() -> {
            assertThat(template.indexExists(INDEX_TEST_1)).isTrue();
            assertThat(template.indexExists(INDEX_TEST_2)).isTrue();
        });
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void deleteIndex_doesNotThrowExceptionIfIndexDoesNotExist() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            assertThatCode(() -> template.deleteIndex(IndexedDocument.class, "not-existing-index"))
                .doesNotThrowAnyException();
        }
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void deleteIndexWithSetName_doesNotThrowExceptionIfIndexDoesNotExist() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            assertThatCode(() -> template.deleteIndex(SET_NAME, "not-existing-index"))
                .doesNotThrowAnyException();
        }
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void createIndex_createsIndexOnNestedList() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            String setName = template.getSetName(IndexedDocument.class);
            template.createIndex(IndexedDocument.class, INDEX_TEST_1, "nestedList", IndexType.STRING,
                IndexCollectionType.LIST, CTX.listIndex(1));

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
    public void createIndex_createsIndexOnNestedListContextRank() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            String setName = template.getSetName(IndexedDocument.class);
            template.createIndex(IndexedDocument.class, INDEX_TEST_1, "nestedList", IndexType.STRING,
                IndexCollectionType.LIST, CTX.listRank(-1));

            awaitTenSecondsUntil(() -> {
                    CTX ctx = Objects.requireNonNull(additionalAerospikeTestOperations.getIndexes(setName).stream()
                        .filter(o -> o.getName().equals(INDEX_TEST_1))
                        .findFirst().orElse(null)).getCtx()[0];

                    assertThat(ctx.id).isEqualTo(CTX.listRank(-1).id);
                    assertThat(ctx.value.toLong()).isEqualTo(CTX.listRank(-1).value.toLong());
                }
            );
        }
    }

    // for Aerospike Server ver. >= 6.1.0.1
    @Test
    public void createIndex_createsIndexOnMapOfMapsContext() {
        if (serverVersionSupport.isDropCreateBehaviorUpdated()) {
            String setName = template.getSetName(IndexedDocument.class);

            CTX[] ctx = new CTX[]{
                CTX.mapKey(com.aerospike.client.Value.get("key1")),
                CTX.mapKey(com.aerospike.client.Value.get("innerKey2"))
            };
            template.createIndex(IndexedDocument.class, INDEX_TEST_1, "mapOfLists", IndexType.STRING,
                IndexCollectionType.MAPKEYS, ctx);

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
                        Exp.ge( // Is the age 18 or older?
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
            String indexName = "idx_selected_names_above_18";
            template.createIndex(SET_NAME, indexName, IndexType.NUMERIC, IndexCollectionType.DEFAULT, filterExp);
            assertThat(template.indexExists(indexName)).isTrue();

            awaitTenSecondsUntil(() ->
                assertThat(additionalAerospikeTestOperations.getIndexes(SET_NAME))
                    .contains(Index.builder().name(indexName).namespace(namespace).set(SET_NAME)
                        .bin("null")
                        .indexType(IndexType.NUMERIC).build())
            );

            // Cleanup
            template.deleteIndex(SET_NAME, indexName);
        }
    }

    @Test
    public void deleteIndex_deletesExistingIndex() {
        template.createIndex(IndexedDocument.class, INDEX_TEST_1, "stringField", IndexType.STRING);
        awaitTenSecondsUntil(() -> assertThat(template.indexExists(INDEX_TEST_1)).isTrue());

        template.deleteIndex(IndexedDocument.class, INDEX_TEST_1);
        awaitTenSecondsUntil(() -> assertThat(template.indexExists(INDEX_TEST_1)).isFalse());
    }

    @Test
    void indexedAnnotation_createsIndexes() {
        AutoIndexedDocumentAssert.assertIndexesCreated(additionalAerospikeTestOperations, namespace, "auto-indexed-set");
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
