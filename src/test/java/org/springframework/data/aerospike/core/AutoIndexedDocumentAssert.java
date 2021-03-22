package org.springframework.data.aerospike.core;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.experimental.UtilityClass;
import org.springframework.data.aerospike.AdditionalAerospikeTestOperations;
import org.springframework.data.aerospike.query.model.Index;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@UtilityClass
public class AutoIndexedDocumentAssert {

    public void assertIndexesCreated(AdditionalAerospikeTestOperations operations, String namespace) {
        assertAutoIndexedDocumentIndexesCreated(operations, namespace);
        assertConfigPackageDocumentIndexesCreated(operations, namespace);
    }

    public void assertAutoIndexedDocumentIndexesCreated(AdditionalAerospikeTestOperations operations, String namespace) {
        String setName = "auto-indexed-set";
        List<Index> indexes = operations.getIndexes(setName);

        assertThat(indexes).containsExactlyInAnyOrder(
                index(namespace, setName, setName + "_someField_string_default", "someField", IndexType.STRING, IndexCollectionType.DEFAULT),
                index(namespace, setName, setName + "_shortName_string_default", "shortName", IndexType.STRING, IndexCollectionType.DEFAULT),
                index(namespace, setName, "overridden_index_name", "customIndexName", IndexType.NUMERIC, IndexCollectionType.DEFAULT),
                index(namespace, setName, "precreated_index", "preCreatedIndex", IndexType.NUMERIC, IndexCollectionType.DEFAULT),
                index(namespace, setName, "placeholder_index1", "placeHolderIdx", IndexType.STRING, IndexCollectionType.DEFAULT),
                index(namespace, setName, setName + "_listOfStrings_string_list", "listOfStrings", IndexType.STRING, IndexCollectionType.LIST),
                index(namespace, setName, setName + "_listOfInts_numeric_list", "listOfInts", IndexType.NUMERIC, IndexCollectionType.LIST),
                index(namespace, setName, setName + "_mapOfStrKeys_string_mapkeys", "mapOfStrKeys", IndexType.STRING, IndexCollectionType.MAPKEYS),
                index(namespace, setName, setName + "_mapOfStrVals_string_mapvalues", "mapOfStrVals", IndexType.STRING, IndexCollectionType.MAPVALUES),
                index(namespace, setName, setName + "_mapOfIntKeys_numeric_mapkeys", "mapOfIntKeys", IndexType.NUMERIC, IndexCollectionType.MAPKEYS),
                index(namespace, setName, setName + "_mapOfIntVals_numeric_mapvalues", "mapOfIntVals", IndexType.NUMERIC, IndexCollectionType.MAPVALUES)
        );
    }

    public void assertConfigPackageDocumentIndexesCreated(AdditionalAerospikeTestOperations operations, String namespace) {
        String setName = "config-package-document-set";
        List<Index> indexes = operations.getIndexes(setName);

        assertThat(indexes).containsExactlyInAnyOrder(
                index(namespace, setName, "config-package-document-index", "indexedField", IndexType.STRING, IndexCollectionType.DEFAULT)
        );
    }

    private static Index index(String namespace, String setName, String name, String bin, IndexType indexType, IndexCollectionType collectionType) {
        return Index.builder()
                .namespace(namespace)
                .set(setName)
                .name(name)
                .bin(bin)
                .indexType(indexType)
                .indexCollectionType(collectionType)
                .build();
    }
}
