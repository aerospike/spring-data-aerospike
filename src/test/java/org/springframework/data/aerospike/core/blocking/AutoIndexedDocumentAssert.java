package org.springframework.data.aerospike.core.blocking;

import com.aerospike.client.cdt.CTX;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.util.AdditionalAerospikeTestOperations;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@UtilityClass
public class AutoIndexedDocumentAssert {

    public void assertIndexesCreated(AdditionalAerospikeTestOperations operations, String namespace, String setName) {
        assertAutoIndexedDocumentIndexesCreated(operations, namespace, setName);
        assertConfigPackageDocumentIndexesCreated(operations, namespace);
    }

    public void assertAutoIndexedDocumentIndexesCreated(AdditionalAerospikeTestOperations operations,
                                                        String namespace, String setName) {
        List<Index> indexes = operations.getIndexes(setName);
        assertThat(indexes).contains(
            index(namespace, setName, "pre_created_index", "preCreatedIndex", IndexType.NUMERIC, null, null),
            index(namespace, setName, "auto-indexed-set_mapOfStrVals_string_mapvalues", "mapOfStrVals", IndexType.STRING, IndexCollectionType.MAPVALUES, null),
            index(namespace, setName, "auto-indexed-set_mapOfIntKeys_numeric_mapkeys", "mapOfIntKeys", IndexType.NUMERIC, IndexCollectionType.MAPKEYS, null)
        );
    }

    public void assertConfigPackageDocumentIndexesCreated(AdditionalAerospikeTestOperations operations,
                                                          String namespace) {
        String setName = "config-package-document-set";
        List<Index> indexes = operations.getIndexes(setName);

        assertThat(indexes).containsExactlyInAnyOrder(
            index(namespace, setName, "config-package-document-index", "indexedField", IndexType.STRING, null, null)
        );
    }

    private static Index index(String namespace, String setName, String name, String bin, IndexType indexType,
                               IndexCollectionType collectionType) {
        return index(namespace, setName, name, bin, indexType, collectionType, new CTX[0]);
    }

    private static Index index(String namespace, String setName, String name, String bin, IndexType indexType,
                               IndexCollectionType collectionType, CTX[] ctx) {
        return Index.builder()
            .namespace(namespace)
            .set(setName)
            .name(name)
            .bin(bin)
            .indexType(indexType)
            .indexCollectionType(collectionType)
            .ctx(ctx)
            .build();
    }
}
