package org.springframework.data.aerospike.mapping;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpiration;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAndExpression;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationAnnotation;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationExpression;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpirationUnit;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpressionInCollection;
import org.springframework.data.aerospike.SampleClasses.DocumentWithUnixTimeExpiration;
import org.springframework.data.aerospike.SampleClasses.DocumentWithoutAnnotation;
import org.springframework.data.aerospike.SampleClasses.DocumentWithoutExpiration;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_SECOND;
import static org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity.DEFAULT_EXPIRATION;

public class AerospikePersistentEntityTest extends BaseBlockingIntegrationTests {

    @Autowired
    private AerospikeMappingContext context;

    @Test
    public void shouldReturnExpirationForDocumentWithExpiration() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpiration.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
    }

    @Test
    public void shouldReturnExpirationForDocumentWithExpirationExpression() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationExpression.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(EXPIRATION_ONE_SECOND);
    }

    @Test
    public void shouldReturnExpirationForDocumentWithExpirationUnit() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationUnit.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo((int) TimeUnit.MINUTES.toSeconds(1));
    }

    @Test
    public void shouldReturnZeroForDocumentWithoutExpiration() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithoutExpiration.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(DEFAULT_EXPIRATION);
    }

    @Test
    public void shouldReturnZeroForDocumentWithoutAnnotation() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithoutAnnotation.class);
        assertThat(persistentEntity.getExpiration()).isEqualTo(DEFAULT_EXPIRATION);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailForDocumentWithExpirationAndExpression() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationAndExpression.class);
        persistentEntity.getExpiration();
    }

    @Test
    public void shouldGetExpirationProperty() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpirationAnnotation.class);
        AerospikePersistentProperty expirationProperty = persistentEntity.getExpirationProperty();
        assertThat(expirationProperty).isNotNull();
        assertThat(expirationProperty.isExpirationProperty()).isTrue();
        assertThat(expirationProperty.isExpirationSpecifiedAsUnixTime()).isFalse();
    }

    @Test
    public void shouldGetExpirationPropertySpecifiedAsUnixTime() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithUnixTimeExpiration.class);
        AerospikePersistentProperty expirationProperty = persistentEntity.getExpirationProperty();
        assertThat(expirationProperty).isNotNull();
        assertThat(expirationProperty.isExpirationProperty()).isTrue();
        assertThat(expirationProperty.isExpirationSpecifiedAsUnixTime()).isTrue();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailForNonExpirationProperty() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithUnixTimeExpiration.class);
        AerospikePersistentProperty expirationProperty = persistentEntity.getIdProperty();
        expirationProperty.isExpirationSpecifiedAsUnixTime();
    }

    @Test
    public void shouldResolvePlaceholdersInCollection() {
        BasicAerospikePersistentEntity<?> persistentEntity = context.getPersistentEntity(DocumentWithExpressionInCollection.class);
        assertThat(persistentEntity.getSetName()).isEqualTo(DocumentWithExpressionInCollection.COLLECTION_PREFIX + "service1");
    }
}
