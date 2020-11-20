package org.springframework.data.aerospike.index;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.AutoIndexedDocument;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReactiveAerospikePersistenceEntityIndexCreatorTest {

    @Mock
    ReactiveAerospikeTemplate template;

    @InjectMocks
    ReactiveAerospikePersistenceEntityIndexCreator creator;

    String name = "someName";
    String fieldName = "fieldName";
    Class<?> targetClass = AutoIndexedDocument.class;
    IndexType type = IndexType.STRING;
    IndexCollectionType collectionType = IndexCollectionType.DEFAULT;
    AerospikeIndexDefinition definition = AerospikeIndexDefinition.builder()
            .name(name)
            .fieldName(fieldName)
            .entityClass(targetClass)
            .type(type)
            .collectionType(collectionType)
            .build();

    @Test
    void shouldInstallIndex() {
        when(template.createIndex(targetClass, name, fieldName, type, collectionType)).thenReturn(Mono.empty());

        Set<AerospikeIndexDefinition> indexes = Collections.singleton(definition);

        creator.installIndexes(indexes);
    }

    @Test
    void shouldSkipInstallIndexOnAlreadyExists() {
        when(template.createIndex(targetClass, name, fieldName, type, collectionType))
                .thenReturn(Mono.error(new IndexAlreadyExistsException("some message", new RuntimeException())));

        Set<AerospikeIndexDefinition> indexes = Collections.singleton(definition);

        creator.installIndexes(indexes);
    }

    @Test
    void shouldFailInstallIndexOnUnhandledException() {
        when(template.createIndex(targetClass, name, fieldName, type, collectionType))
                .thenReturn(Mono.error(new RuntimeException()));

        Set<AerospikeIndexDefinition> indexes = Collections.singleton(definition);

        assertThrows(RuntimeException.class, () -> creator.installIndexes(indexes));
    }
}