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
package org.springframework.data.aerospike.repository.support;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.aerospike.core.AerospikeOperations;
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
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@MockitoSettings(strictness = Strictness.LENIENT)
@ExtendWith(MockitoExtension.class)
public class AerospikeRepositoryFactoryTest {

	@Mock RepositoryInformation repositoryInformation;
	@SuppressWarnings("rawtypes")
	@Mock MappingContext context;
	@Mock AerospikeRepositoryFactory aerospikeRepositoryFactoryMock;
	@SuppressWarnings("rawtypes")
	@Mock AerospikePersistentEntity entity;
	@Mock AerospikeOperations aerospikeOperations;

	@SuppressWarnings("unchecked")
	@BeforeEach
	public void setUp() {
		when(aerospikeOperations.getMappingContext()).thenReturn(context);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getEntityInformationClassOfT() {
		when(context.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		AerospikeRepositoryFactory factory = new AerospikeRepositoryFactory(aerospikeOperations);
		EntityInformation<Person, Serializable> entityInformation = factory.getEntityInformation(Person.class);
		assertThat(entityInformation).isInstanceOf(PersistentEntityInformation.class);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void getTargetRepositoryRepositoryInformation() {
		when(aerospikeRepositoryFactoryMock.getTargetRepository(repositoryInformation)).thenReturn(new Object());

		Person.class.getDeclaredConstructors();

		Object repository = aerospikeRepositoryFactoryMock.getTargetRepository(repositoryInformation);
		assertThat(repository).isNotNull();
	}

	@Test
	public void testGetRepositoryBaseClassRepositoryMetadata() {
		RepositoryMetadata metadata = mock(RepositoryMetadata.class);
		Mockito.<Class<?>>when(metadata.getRepositoryInterface()).thenReturn(SimpleKeyValueRepository.class);
		AerospikeRepositoryFactory factory = new AerospikeRepositoryFactory(aerospikeOperations);
		Class<?> repbaseClass = factory.getRepositoryBaseClass(metadata);
		assertThat(repbaseClass.getSimpleName()).isEqualTo(SimpleKeyValueRepository.class.getSimpleName());
	}

}
