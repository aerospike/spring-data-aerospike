/*
 * Copyright 2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.mapping;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpressionInCollection;
import org.springframework.data.aerospike.SampleClasses.DocumentWithoutCollection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
public class BasicAerospikePersistentEntityTest {

    private final AerospikeMappingContext context = new AerospikeMappingContext();

    @Test
    public void shouldReturnSimpleClassNameIfCollectionNotSpecified() {
        BasicAerospikePersistentEntity<?> entity = context.getPersistentEntity(DocumentWithoutCollection.class);

        assertThat(entity.getSetName()).isEqualTo(DocumentWithoutCollection.class.getSimpleName());
    }

    @Test
    public void shouldFailIfEnvironmentNull() {
        BasicAerospikePersistentEntity<?> entity = context.getPersistentEntity(DocumentWithExpressionInCollection.class);

        assertThatThrownBy(entity::getSetName)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Environment must be set to use 'collection'");
    }
}
