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
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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

        Person result = aerospikeRepository.save(testPerson).block();

        assertThat(testPerson).isEqualTo(result);
    }

    @Test
    public void testSaveAllIterable() {
        when(operations.save(any(Person.class))).then(invocation -> Mono.just(invocation.getArgument(0)));

        List<Person> result = aerospikeRepository.saveAll(testPersons).collectList().block();

        assertThat(result).containsOnlyElementsOf(testPersons);
        verify(operations, times(testPersons.size())).save(any(Person.class));
    }

    @Test
    public void testSaveAllPublisher() {
        when(operations.save(any(Person.class))).then(invocation -> Mono.just(invocation.getArgument(0)));

        List<Person> result = aerospikeRepository.saveAll(Flux.fromIterable(testPersons)).collectList().block();

        assertThat(result).containsOnlyElementsOf(testPersons);
        verify(operations, times(testPersons.size())).save(any(Person.class));
    }

    @Test
    public void testFindById() {
        when(metadata.getJavaType()).thenReturn(Person.class);
        when(operations.findById("21", Person.class)).thenReturn(Mono.just(testPerson));

        Person result = aerospikeRepository.findById("21").block();

        assertThat(result).isEqualTo(testPerson);
    }

    @Test
    public void testFindByIdPublisher() {
        List<String> ids = asList("21", "one", "two", "three");

        when(metadata.getJavaType()).thenReturn(Person.class);
        when(operations.findById("21", Person.class)).thenReturn(Mono.just(testPerson));

        Person result = aerospikeRepository.findById(Flux.fromIterable(ids)).block();

        assertThat(result).isEqualTo(testPerson);
    }


    @Test
    public void testFindAll() {
        when(metadata.getJavaType()).thenReturn(Person.class);
        when(operations.findAll(Person.class)).thenReturn(Flux.fromIterable(testPersons));

        List<Person> result = aerospikeRepository.findAll().collectList().block();

        assertThat(result).containsOnlyElementsOf(testPersons);
    }

    @Test
    public void testFindAllByIdIterable() {
        List<String> ids = testPersons.stream().map(Person::getId).collect(toList());
        when(metadata.getJavaType()).thenReturn(Person.class);
        when(aerospikeRepository.findAllById(ids)).thenReturn(Flux.fromIterable(testPersons));

        List<Person> result = aerospikeRepository.findAllById(ids).collectList().block();

        assertThat(result).containsOnlyElementsOf(testPersons);
    }

    @Test
    public void testFindAllByIdPublisher() {
        Map<String, Person> id2person = testPersons.stream().collect(toMap(Person::getId, person -> person));
        when(metadata.getJavaType()).thenReturn(Person.class);
        when(operations.findById(any(String.class), eq(Person.class)))
                .then(invocation -> Mono.just(id2person.get(invocation.getArgument(0))));

        List<Person> result = aerospikeRepository.findAllById(Flux.fromIterable(id2person.keySet())).collectList().block();

        assertThat(result).containsOnlyElementsOf(testPersons);
    }


}
