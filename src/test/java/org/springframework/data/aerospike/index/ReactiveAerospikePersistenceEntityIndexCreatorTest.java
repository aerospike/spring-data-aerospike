package org.springframework.data.aerospike.index;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.exceptions.IndexAlreadyExistsException;
import org.springframework.data.aerospike.sample.AutoIndexedDocument;
import org.springframework.data.aerospike.utility.MockObjectProvider;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
class ReactiveAerospikePersistenceEntityIndexCreatorTest {

    final boolean createIndexesOnStartup = true;
    final AerospikeIndexResolver aerospikeIndexResolver = mock(AerospikeIndexResolver.class);
    final ReactiveAerospikeTemplate template = mock(ReactiveAerospikeTemplate.class);

    final ReactiveAerospikePersistenceEntityIndexCreator creator =
        new ReactiveAerospikePersistenceEntityIndexCreator(null, createIndexesOnStartup, aerospikeIndexResolver,
            new MockObjectProvider<>(template));

    final String name = "someName";
    final String fieldName = "fieldName";
    final Class<?> targetClass = AutoIndexedDocument.class;
    final IndexType type = IndexType.STRING;
    final IndexCollectionType collectionType = IndexCollectionType.DEFAULT;
    final AerospikeIndexDefinition definition = AerospikeIndexDefinition.builder()
        .name(name)
        .bin(fieldName)
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
