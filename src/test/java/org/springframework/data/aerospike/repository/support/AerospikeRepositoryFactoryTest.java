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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PersistentEntityInformation;

import java.io.Serializable;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class AerospikeRepositoryFactoryTest {

	@Mock RepositoryInformation repositoryInformation;
	@SuppressWarnings("rawtypes")
	@Mock MappingContext context;
	@Mock AerospikeRepositoryFactory aerospikeRepositoryFactoryMock;
	@SuppressWarnings("rawtypes")
	@Mock AerospikePersistentEntity entity;
	@Mock AerospikeOperations aerospikeOperations;

	@Rule public ExpectedException exception = ExpectedException.none();

	/**
	 * @throws java.lang.Exception
	 */
	@SuppressWarnings("unchecked")
	@Before
	public void setUp() throws Exception {
		when(aerospikeOperations.getMappingContext()).thenReturn(context);

	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.AerospikeRepositoryFactory#getEntityInformation(java.lang.Class)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testGetEntityInformationClassOfT() {
		when(context.getRequiredPersistentEntity(Person.class)).thenReturn(entity);

		AerospikeRepositoryFactory factory = new AerospikeRepositoryFactory(aerospikeOperations);
		EntityInformation<Person, Serializable> entityInformation = factory.getEntityInformation(Person.class);
		assertTrue(entityInformation instanceof PersistentEntityInformation);
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.AerospikeRepositoryFactory#getTargetRepository(org.springframework.data.repository.core.RepositoryInformation)}.
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testGetTargetRepositoryRepositoryInformation() {
		when(aerospikeRepositoryFactoryMock.getTargetRepository(repositoryInformation)).thenReturn(new Object());

		Person.class.getDeclaredConstructors();

		Object repository = aerospikeRepositoryFactoryMock.getTargetRepository(repositoryInformation);
		assertThat(repository, is(notNullValue()));
	}

	/**
	 * Test method for {@link org.springframework.data.aerospike.repository.support.AerospikeRepositoryFactory#getRepositoryBaseClass(org.springframework.data.repository.core.RepositoryMetadata)}.
	 */
	@Test
	public void testGetRepositoryBaseClassRepositoryMetadata() {
		RepositoryMetadata metadata = mock(RepositoryMetadata.class);
		Mockito.<Class<?>>when(metadata.getRepositoryInterface()).thenReturn(SimpleKeyValueRepository.class);
		AerospikeRepositoryFactory factory = new AerospikeRepositoryFactory(aerospikeOperations);
		Class<?> repbaseClass = factory.getRepositoryBaseClass(metadata);
		assertTrue(repbaseClass.getSimpleName().equals(SimpleKeyValueRepository.class.getSimpleName()));
	}

}
