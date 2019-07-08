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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.core.ReactiveAerospikeOperations;
import org.springframework.data.repository.core.EntityInformation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Igor Ermolenko
 */
@RunWith(MockitoJUnitRunner.StrictStubs.class)
public class SimpleReactiveAerospikeRepositoryTest {

	@Mock
	EntityInformation<Person, String> metadata;
	@Mock
	ReactiveAerospikeOperations operations;
	@InjectMocks
	SimpleReactiveAerospikeRepository<Person, String> aerospikeRepository;

	private Person testPerson;
	private List<Person> testPersons;

	@Rule
	public ExpectedException exception = ExpectedException.none();


	@Before
	public void setUp() {
		testPerson = new Person("21", "Jean");
		testPersons = asList(
				new Person("one", "Jean", 21),
				new Person("two", "Jean2", 22),
				new Person("three", "Jean3", 23));
	}

	@Test
	public void testSave() {
		when(operations.save(testPerson)).thenReturn(Mono.just(testPerson));

		Person myPerson = aerospikeRepository.save(testPerson).block();

		assertThat(testPerson).isEqualTo(myPerson);
	}

    @Test
    public void testSaveAllIterable() {
        when(operations.save(any(Person.class))).then(invocation -> Mono.just(invocation.getArgument(0)));

        List<Person> result = aerospikeRepository.saveAll(testPersons).collectList().block();

        assertThat(result).isEqualTo(testPersons);
        verify(operations, times(testPersons.size())).save(any(Person.class));
    }

    @Test
    public void testSaveAllPublisher() {
        when(operations.save(any(Person.class))).then(invocation -> Mono.just(invocation.getArgument(0)));

        List<Person> result = aerospikeRepository.saveAll(Flux.fromIterable(testPersons)).collectList().block();

        assertThat(result).isEqualTo(testPersons);
        verify(operations, times(testPersons.size())).save(any(Person.class));
    }

}
