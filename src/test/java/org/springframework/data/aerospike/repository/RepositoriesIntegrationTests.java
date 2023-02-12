/*
 * Copyright 2012-2018 the original author or authors
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
package org.springframework.data.aerospike.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.CompositeObject;
import org.springframework.data.aerospike.sample.CompositeObjectRepository;
import org.springframework.data.aerospike.sample.SimpleObject;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class RepositoriesIntegrationTests extends BaseBlockingIntegrationTests {

    @Autowired
    CompositeObjectRepository repository;

    @Test
    public void findOne_shouldReturnNullForNonExistingKey() {
        Optional<CompositeObject> one = repository.findById("non-existing-id");

        assertThat(one).isNotPresent();
    }

    @Test
    public void shouldSaveObjectWithPersistenceConstructorThatHasAllFields() {
        CompositeObject expected = CompositeObject.builder()
                .id("composite-object-1")
                .intValue(15)
                .simpleObject(SimpleObject.builder().property1("prop1").property2(555).build())
                .build();
        repository.save(expected);

        Optional<CompositeObject> actual = repository.findById(expected.getId());

        assertThat(actual).hasValue(expected);
    }

    @Test
    public void shouldDeleteObjectWithPersistenceConstructor() {
        String id = nextId();
        CompositeObject expected = CompositeObject.builder()
                .id(id)
                .build();
        repository.save(expected);
        assertThat(repository.findById(id)).isPresent();

        repository.delete(expected);

        assertThat(repository.findById(id)).isNotPresent();
    }
}
