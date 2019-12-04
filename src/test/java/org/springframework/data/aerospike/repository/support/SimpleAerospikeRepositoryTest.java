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

import com.aerospike.client.query.IndexType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.repository.core.EntityInformation;

import java.util.List;
import java.util.Optional;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class SimpleAerospikeRepositoryTest {

	@Mock
	EntityInformation<Person, String> metadata;
	@Mock
	AerospikeOperations operations;
	@InjectMocks
	SimpleAerospikeRepository<Person, String> aerospikeRepository;

	Person testPerson;
	List<Person> testPersons;

	@Rule
	public ExpectedException exception = ExpectedException.none();


	@Before
	public void setUp() throws Exception {
		when(metadata.getJavaType()).thenReturn(Person.class);

		testPerson = new Person("21", "Jean");
		testPersons = asList(
				new Person("one", "Jean", 21),
				new Person("two", "Jean2", 22),
				new Person("three", "Jean3", 23));
	}

	@Test
	public void testFindOne() {
		when(operations.findById("21", Person.class)).thenReturn(testPerson);

		Optional<Person> person = aerospikeRepository.findById("21");

		assertThat(person)
				.hasValueSatisfying(actual -> assertThat(actual.getFirstName()).isEqualTo("Jean"));
	}

	@Test
	public void testSave() {
		Person myPerson = aerospikeRepository.save(testPerson);

		assertThat(testPerson).isEqualTo(myPerson);
		verify(operations).save(testPerson);
	}

	@Test
	public void testSaveIterableOfS() {
		List<Person> result = aerospikeRepository.saveAll(testPersons);

		assertThat(result).isEqualTo(testPersons);
		verify(operations, times(testPersons.size())).save(any());
	}

	@Test
	public void testDelete() {
		aerospikeRepository.delete(testPerson);

		verify(operations).delete(testPerson);
	}

	@Test
	public void testFindAllSort() {
		when(operations.findAll(Sort.by(Sort.Direction.ASC, "biff"), Person.class)).thenReturn(testPersons);

		Iterable<Person> fetchList = aerospikeRepository.findAll(Sort.by(Sort.Direction.ASC, "biff"));
		assertThat(fetchList).isEqualTo(testPersons);
	}

	@Test
	public void testFindAllPageable() {
		Page<Person> page = new PageImpl<>(IterableConverter.toList(testPersons), PageRequest.of(0, 2), 5);

		doReturn(testPersons.stream()).when(operations).findInRange(0, 2, Sort.unsorted(), Person.class);
		doReturn("set").when(operations).getSetName(Person.class);
		doReturn(5L).when(operations).count(Person.class, "set");

		Page<Person> result = aerospikeRepository.findAll(PageRequest.of(0, 2));

		verify(operations).findInRange(0, 2, Sort.unsorted(), Person.class);
		assertThat(result).isEqualTo(page);
	}

	@Test
	public void testExists() {
		when(operations.exists(testPerson.getId(), Person.class)).thenReturn(true);

		boolean exists = aerospikeRepository.existsById(testPerson.getId());
		assertThat(exists).isTrue();
	}

	@Test
	public void testFindAll() {
		when(operations.findAll(Person.class)).thenReturn(testPersons.stream());

		List<Person> fetchList = aerospikeRepository.findAll();

		assertThat(fetchList).containsOnlyElementsOf(testPersons);
	}

	@Test
	public void testFindAllIterableOfID() {
		List<String> ids = testPersons.stream().map(Person::getId).collect(toList());
		when(aerospikeRepository.findAllById(ids)).thenReturn(testPersons);

		List<Person> fetchList = (List<Person>) aerospikeRepository.findAllById(ids);

		assertThat(fetchList).isEqualTo(testPersons);
	}

	@Test
	public void testDeleteID() {
		aerospikeRepository.deleteById("one");

		verify(operations).delete("one", Person.class);
	}

	@Test
	public void testDeleteIterableOfQextendsT() {
		aerospikeRepository.deleteAll(testPersons);

		verify(operations, times(testPersons.size())).delete(any(Person.class));
	}

	@Test
	public void testDeleteAll() {
		aerospikeRepository.deleteAll();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {}
	}

	@Test
	public void testCreateIndex() {
		aerospikeRepository.createIndex(Person.class, "index_first_name", "firstName", IndexType.STRING);

		verify(operations).createIndex(Person.class, "index_first_name", "firstName", IndexType.STRING);
	}

	@Test
	public void testDeleteIndex() throws Exception {
		aerospikeRepository.deleteIndex(Person.class, "index_first_name");

		verify(operations).deleteIndex(Person.class, "index_first_name");
	}

	@Test
	public void testIndexExists() throws Exception {
		when(operations.indexExists(anyString())).thenReturn(true);

		boolean exists = aerospikeRepository.indexExists("index_first_name");

		assertThat(exists).isTrue();
		verify(operations).indexExists("index_first_name");
	}
}
