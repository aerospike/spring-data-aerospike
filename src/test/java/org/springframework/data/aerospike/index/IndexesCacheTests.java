package org.springframework.data.aerospike.index;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.annotation.Nightly;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;

import java.util.List;
import java.util.Optional;

import static com.aerospike.client.query.IndexCollectionType.LIST;
import static com.aerospike.client.query.IndexCollectionType.MAPKEYS;
import static com.aerospike.client.query.IndexCollectionType.MAPVALUES;
import static com.aerospike.client.query.IndexType.GEO2DSPHERE;
import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;
import static org.assertj.core.api.Assertions.assertThat;

@Nightly
public class IndexesCacheTests extends BaseBlockingIntegrationTests {

    private static final String SET = "index-test";
    private static final String BIN_1 = "bin-1";
    private static final String BIN_2 = "bin-2";
    private static final String BIN_3 = "bin-3";
    private static final String INDEX_NAME = "index-1";
    private static final String INDEX_NAME_2 = "index-2";
    private static final String INDEX_NAME_3 = "index-3";

    @Override
    @BeforeEach
    public void setUp() {
        List<Index> dropIndexes = List.of(
            Index.builder().set(SET).name(INDEX_NAME).build(),
            Index.builder().set(null).name(INDEX_NAME_2).build(),
            Index.builder().set(SET).name(INDEX_NAME_2).build(),
            Index.builder().set(SET).name(INDEX_NAME_3).build()
        );
        additionalAerospikeTestOperations.dropIndexes(dropIndexes);
    }

    @Test
    public void refreshIndexes_findsNewlyCreatedIndex() {
        Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null));
        assertThat(index).isEmpty();

        additionalAerospikeTestOperations.createIndex(namespace, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC);

        index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null));
        assertThat(index).isPresent()
            .hasValueSatisfying(value -> {
                assertThat(value.getName()).isEqualTo(INDEX_NAME);
                assertThat(value.getNamespace()).isEqualTo(namespace);
                assertThat(value.getSet()).isEqualTo(SET);
                assertThat(value.getBin()).isEqualTo(BIN_1);
                assertThat(value.getIndexType()).isEqualTo(IndexType.NUMERIC);
            });
    }

    @Test
    public void refreshIndexes_removesDeletedIndex() {
        additionalAerospikeTestOperations.createIndex(namespace, SET, INDEX_NAME, BIN_1, IndexType.NUMERIC);

        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null))).isPresent();

        additionalAerospikeTestOperations.dropIndex(SET, INDEX_NAME);

        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null))).isEmpty();
    }

    @Test
    public void refreshIndexes_indexWithoutSetCanBeParsed() {
        additionalAerospikeTestOperations.createIndex(namespace, null, INDEX_NAME_2, BIN_2, IndexType.STRING);

        Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, null, BIN_2, IndexType.STRING, null));
        assertThat(index).isPresent()
            .hasValueSatisfying(value -> {
                assertThat(value.getName()).isEqualTo(INDEX_NAME_2);
                assertThat(value.getNamespace()).isEqualTo(namespace);
                assertThat(value.getSet()).isNull();
                assertThat(value.getBin()).isEqualTo(BIN_2);
                assertThat(value.getIndexType()).isEqualTo(IndexType.STRING);
            });
    }

    @Test
    public void refreshIndexes_indexWithGeoTypeCanBeParsed() {
        additionalAerospikeTestOperations.createIndex(namespace, SET, INDEX_NAME_3, BIN_3, GEO2DSPHERE);

        Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_3, GEO2DSPHERE, null));
        assertThat(index).isPresent()
            .hasValueSatisfying(value -> {
                assertThat(value.getName()).isEqualTo(INDEX_NAME_3);
                assertThat(value.getNamespace()).isEqualTo(namespace);
                assertThat(value.getSet()).isEqualTo(SET);
                assertThat(value.getBin()).isEqualTo(BIN_3);
                assertThat(value.getIndexType()).isEqualTo(GEO2DSPHERE);
            });
    }

    @Test
    public void refreshIndexes_multipleIndexesForTheSameBinCanBeParsed() {

        List<Index> newIndexes = List.of(
            Index.builder().set(SET).name(INDEX_NAME).bin(BIN_1).indexType(NUMERIC).indexCollectionType(MAPKEYS).build(),
            Index.builder().set(SET).name(INDEX_NAME_2).bin(BIN_1).indexType(NUMERIC).indexCollectionType(MAPVALUES).build(),
            Index.builder().set(SET).name(INDEX_NAME_3).bin(BIN_2).indexType(NUMERIC).indexCollectionType(LIST).build()
        );
        additionalAerospikeTestOperations.createIndexes(newIndexes);

        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC,
            IndexCollectionType.MAPKEYS))).isPresent();
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC,
            IndexCollectionType.MAPVALUES))).isPresent();
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.NUMERIC,
            IndexCollectionType.LIST))).isPresent();
    }

    @Test
    public void refreshIndexes_multipleIndexesCanBeParsed() {
        List<Index> newIndexes = List.of(
            Index.builder().set(SET).name(INDEX_NAME).bin(BIN_1).indexType(NUMERIC).build(),
            Index.builder().set(null).name(INDEX_NAME_2).bin(BIN_2).indexType(STRING).build(),
            Index.builder().set(SET).name(INDEX_NAME_3).bin(BIN_3).indexType(GEO2DSPHERE).build()
        );
        additionalAerospikeTestOperations.createIndexes(newIndexes);

        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null))).isPresent();
        assertThat(indexesCache.getIndex(new IndexKey(namespace, null, BIN_2, IndexType.STRING, null))).isPresent();
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_3, GEO2DSPHERE, null))).isPresent();
        assertThat(indexesCache.getIndex(new IndexKey("unknown", null, "unknown", IndexType.NUMERIC, null))).isEmpty();
    }

    @Test
    public void refreshIndexes_indexesForTheSameBinCanBeParsed() {
        List<Index> newIndexes = List.of(
            Index.builder().set(SET).name(INDEX_NAME).bin(BIN_1).indexType(NUMERIC).build(),
            Index.builder().set(SET).name(INDEX_NAME_2).bin(BIN_1).indexType(STRING).build()
        );
        additionalAerospikeTestOperations.createIndexes(newIndexes);

        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null))).hasValueSatisfying(value -> {
            assertThat(value.getName()).isEqualTo(INDEX_NAME);
            assertThat(value.getNamespace()).isEqualTo(namespace);
            assertThat(value.getSet()).isEqualTo(SET);
            assertThat(value.getBin()).isEqualTo(BIN_1);
            assertThat(value.getIndexType()).isEqualTo(IndexType.NUMERIC);
        });
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.STRING, null))).hasValueSatisfying(value -> {
            assertThat(value.getName()).isEqualTo(INDEX_NAME_2);
            assertThat(value.getNamespace()).isEqualTo(namespace);
            assertThat(value.getSet()).isEqualTo(SET);
            assertThat(value.getBin()).isEqualTo(BIN_1);
            assertThat(value.getIndexType()).isEqualTo(IndexType.STRING);
        });

    }

    @Test
    public void isIndexedBin_returnsTrueForIndexedField() {
        List<Index> newIndexes = List.of(
            Index.builder().set(SET).name(INDEX_NAME).bin(BIN_1).indexType(NUMERIC).build(),
            Index.builder().set(SET).name(INDEX_NAME_2).bin(BIN_2).indexType(NUMERIC).build()
        );
        additionalAerospikeTestOperations.createIndexes(newIndexes);

        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null))).isPresent();
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.NUMERIC, null))).isPresent();
    }

    @Test
    public void isIndexedBin_returnsFalseForNonIndexedField() {
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.NUMERIC, null))).isEmpty();
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, IndexType.STRING, null))).isEmpty();
        assertThat(indexesCache.getIndex(new IndexKey(namespace, SET, BIN_2, GEO2DSPHERE, null))).isEmpty();
    }

    @Test
    public void getIndex_returnsEmptyForNonExistingIndex() {
        Optional<Index> index = indexesCache.getIndex(new IndexKey(namespace, SET, BIN_1, IndexType.NUMERIC, null));

        assertThat(index).isEmpty();
    }
}
