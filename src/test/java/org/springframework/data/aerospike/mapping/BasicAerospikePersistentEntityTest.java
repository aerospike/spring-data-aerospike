package org.springframework.data.aerospike.mapping;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpressionInCollection;
import org.springframework.data.aerospike.SampleClasses.DocumentWithoutCollection;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class BasicAerospikePersistentEntityTest {

    private AerospikeMappingContext context = new AerospikeMappingContext();

    @Test
    public void shouldReturnSimpleClassNameIfCollectionNotSpecified() {
        BasicAerospikePersistentEntity<?> entity = context.getPersistentEntity(DocumentWithoutCollection.class);

        assertThat(entity.getSetName()).isEqualTo(DocumentWithoutCollection.class.getSimpleName());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailIfEnvironmentNull() {
        BasicAerospikePersistentEntity<?> entity = context.getPersistentEntity(DocumentWithExpressionInCollection.class);

        entity.getSetName();
    }

}
