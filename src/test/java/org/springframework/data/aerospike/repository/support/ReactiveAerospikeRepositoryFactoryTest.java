/*
 * Copyright 2012-2019 the original author or authors
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
package org.springframework.data.aerospike.repository.support;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.aerospike.core.ReactiveAerospikeOperations;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PersistentEntityInformation;

import java.io.Serializable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Igor Ermolenko
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class ReactiveAerospikeRepositoryFactoryTest {

    @Mock
    RepositoryInformation repositoryInformation;
    @SuppressWarnings("rawtypes")
    @Mock
    MappingContext context;
    @Mock
    ReactiveAerospikeRepositoryFactory aerospikeRepositoryFactoryMock;
    @SuppressWarnings("rawtypes")
    @Mock
    AerospikePersistentEntity entity;
    @Mock
    ReactiveAerospikeOperations aerospikeOperations;

    @BeforeEach
    public void setUp() {
        when(aerospikeOperations.getMappingContext()).thenReturn(context);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getEntityInformationClassOfT() {
        when(context.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

        ReactiveAerospikeRepositoryFactory factory = new ReactiveAerospikeRepositoryFactory(aerospikeOperations);
        EntityInformation<Person, Serializable> entityInformation = factory.getEntityInformation(Person.class);
        assertThat(entityInformation).isInstanceOf(PersistentEntityInformation.class);
    }

    @Test
    public void getTargetRepositoryRepositoryInformation() {
        when(aerospikeRepositoryFactoryMock.getTargetRepository(repositoryInformation)).thenReturn(new Object());

        Object repository = aerospikeRepositoryFactoryMock.getTargetRepository(repositoryInformation);
        assertThat(repository).isNotNull();
    }

    @Test
    public void getRepositoryBaseClassRepositoryMetadata() {
        RepositoryMetadata metadata = mock(RepositoryMetadata.class);
        Mockito.<Class<?>>when(metadata.getRepositoryInterface()).thenReturn(SimpleKeyValueRepository.class);

        ReactiveAerospikeRepositoryFactory factory = new ReactiveAerospikeRepositoryFactory(aerospikeOperations);
        Class<?> repositoryBaseClass = factory.getRepositoryBaseClass(metadata);

        assertThat(repositoryBaseClass.getSimpleName()).isEqualTo(SimpleKeyValueRepository.class.getSimpleName());
    }
}
