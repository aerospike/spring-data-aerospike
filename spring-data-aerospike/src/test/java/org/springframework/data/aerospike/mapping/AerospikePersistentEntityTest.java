package org.springframework.data.aerospike.mapping;

import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.*;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.*;
import static org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity.DEFAULT_EXPIRATION;

public class AerospikePersistentEntityTest extends BaseIntegrationTests {

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
}
