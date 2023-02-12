package org.springframework.data.aerospike.index;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.sample.AutoIndexedDocument;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class AerospikePersistenceEntityIndexCreatorTest {

    final boolean createIndexesOnStartup = true;
    final AerospikeIndexResolver aerospikeIndexResolver = mock(AerospikeIndexResolver.class);
    final AerospikeTemplate template = mock(AerospikeTemplate.class);

    final AerospikePersistenceEntityIndexCreator creator =
        new AerospikePersistenceEntityIndexCreator(null, createIndexesOnStartup, aerospikeIndexResolver, template);

    final String name = "someName";
    final String fieldName = "fieldName";
    final Class<?> targetClass = AutoIndexedDocument.class;
    final IndexType type = IndexType.STRING;
    final IndexCollectionType collectionType = IndexCollectionType.LIST;
    final AerospikeIndexDefinition definition = AerospikeIndexDefinition.builder()
        .name(name)
        .fieldName(fieldName)
        .entityClass(targetClass)
        .type(type)
        .collectionType(collectionType)
        .build();

    @Test
    void shouldInstallIndex() {
        Set<AerospikeIndexDefinition> indexes = Collections.singleton(definition);

        creator.installIndexes(indexes);

        verify(template).createIndex(targetClass, name, fieldName, type, collectionType);
    }

    @Test
    void shouldSkipInstallIndexOnAlreadyExists() {
        doThrow(new IndexAlreadyExistsException("some message", new RuntimeException()))
            .when(template).createIndex(targetClass, name, fieldName, type, collectionType);

        Set<AerospikeIndexDefinition> indexes = Collections.singleton(definition);

        creator.installIndexes(indexes);

        verify(template).createIndex(targetClass, name, fieldName, type, collectionType);
    }

    @Test
    void shouldFailInstallIndexOnUnhandledException() {
        doThrow(new RuntimeException())
            .when(template).createIndex(targetClass, name, fieldName, type, collectionType);

        Set<AerospikeIndexDefinition> indexes = Collections.singleton(definition);

        assertThrows(RuntimeException.class, () -> creator.installIndexes(indexes));

        verify(template).createIndex(targetClass, name, fieldName, type, collectionType);
    }
}
