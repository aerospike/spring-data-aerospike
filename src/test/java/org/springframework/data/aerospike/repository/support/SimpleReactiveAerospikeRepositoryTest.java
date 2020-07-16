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

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.aerospike.core.ReactiveAerospikeOperations;
import org.springframework.data.aerospike.sample.Customer;
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
@ExtendWith(MockitoExtension.class)
public class SimpleReactiveAerospikeRepositoryTest {

    @Mock
    EntityInformation<Customer, String> metadata;
    @Mock
    ReactiveAerospikeOperations operations;
    @InjectMocks
    SimpleReactiveAerospikeRepository<Customer, String> repository;

    private Customer testCustomer;
    private List<Customer> testCustomers;

    @BeforeEach
    public void setUp() {
        testCustomer = Customer.builder().id("21").firstname("Jean").build();
        testCustomers = asList(
                Customer.builder().id("one").firstname("Jean").age(21).build(),
                Customer.builder().id("two").firstname("Jean2").age(22).build(),
                Customer.builder().id("three").firstname("Jean3").age(23).build());
    }

    @Test
    public void save() {
        when(operations.save(testCustomer)).thenReturn(Mono.just(testCustomer));

        Customer result = repository.save(testCustomer).block();

        assertThat(testCustomer).isEqualTo(result);
    }

    @Test
    public void saveAllIterable() {
        when(operations.save(any(Customer.class))).then(invocation -> Mono.just(invocation.getArgument(0)));

        List<Customer> result = repository.saveAll(testCustomers).collectList().block();

        assertThat(result).hasSameElementsAs(testCustomers);
        verify(operations, times(testCustomers.size())).save(any(Customer.class));
    }

    @Test
    public void saveAllPublisher() {
        when(operations.save(any(Customer.class))).then(invocation -> Mono.just(invocation.getArgument(0)));

        List<Customer> result = repository.saveAll(Flux.fromIterable(testCustomers)).collectList().block();

        assertThat(result).hasSameElementsAs(testCustomers);
        verify(operations, times(testCustomers.size())).save(any(Customer.class));
    }

    @Test
    public void findById() {
        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.findById("21", Customer.class)).thenReturn(Mono.just(testCustomer));

        Customer result = repository.findById("21").block();

        assertThat(result).isEqualTo(testCustomer);
    }

    @Test
    public void testFindByIdPublisher() {
        List<String> ids = asList("21", "one", "two", "three");

        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.findById("21", Customer.class)).thenReturn(Mono.just(testCustomer));

        Customer result = repository.findById(Flux.fromIterable(ids)).block();

        assertThat(result).isEqualTo(testCustomer);
    }


    @Test
    public void testFindAll() {
        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.findAll(Customer.class)).thenReturn(Flux.fromIterable(testCustomers));

        List<Customer> result = repository.findAll().collectList().block();

        assertThat(result).hasSameElementsAs(testCustomers);
    }

    @Test
    public void testFindAllByIdIterable() {
        List<String> ids = testCustomers.stream().map(Customer::getId).collect(toList());
        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(repository.findAllById(ids)).thenReturn(Flux.fromIterable(testCustomers));

        List<Customer> result = repository.findAllById(ids).collectList().block();

        assertThat(result).hasSameElementsAs(testCustomers);
    }

    @Test
    public void testFindAllByIdPublisher() {
        Map<String, Customer> id2person = testCustomers.stream().collect(toMap(Customer::getId, person -> person));
        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.findById(any(String.class), eq(Customer.class)))
                .then(invocation -> Mono.just(id2person.get(invocation.getArgument(0))));

        List<Customer> result = repository.findAllById(Flux.fromIterable(id2person.keySet())).collectList().block();

        assertThat(result).hasSameElementsAs(testCustomers);
    }

    @Test
    public void testExistsById() {
        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.exists(testCustomer.getId(), Customer.class)).thenReturn(Mono.just(true));

        Boolean exists = repository.existsById(testCustomer.getId()).block();
        assertThat(exists).isTrue();
    }

    @Test
    public void testExistsByIdPublisher() {
        List<String> ids = asList("21", "one", "two", "three");

        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.exists("21", Customer.class)).thenReturn(Mono.just(true));

        Boolean exists = repository.existsById(Flux.fromIterable(ids)).block();
        assertThat(exists).isTrue();
    }

    @Test
    public void testDeleteById() {
        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.delete("77", Customer.class)).thenReturn(Mono.just(true));

        repository.deleteById("77").block();
        verify(operations, times(1)).delete("77", Customer.class);
    }

    @Test
    public void testDeleteByIdPublisher() {
        when(metadata.getJavaType()).thenReturn(Customer.class);
        when(operations.delete("77", Customer.class)).thenReturn(Mono.just(true));

        repository.deleteById(Flux.just("77", "88", "99")).block();
        verify(operations, times(1)).delete("77", Customer.class);
    }

    @Test
    public void testDelete() {
        when(operations.delete(testCustomer)).thenReturn(Mono.just(true));

        repository.delete(testCustomer).block();
        verify(operations, times(1)).delete(testCustomer);
    }

    @Test
    public void testDeleteAllIterable() {
        when(operations.delete(any(Customer.class))).thenReturn(Mono.just(true));

        repository.deleteAll(testCustomers).block();
        verify(operations, times(testCustomers.size())).delete(any(Customer.class));
    }

    @Test
    public void testDeleteAllPublisher() {
        when(operations.delete(any(Customer.class))).thenReturn(Mono.just(true));

        repository.deleteAll(Flux.fromIterable(testCustomers)).block();
        verify(operations, times(testCustomers.size())).delete(any(Customer.class));
    }

    @Test
    public void testCreateIndex() {
        repository.createIndex(Customer.class, "index_first_name", "firstName", IndexType.STRING);

        verify(operations).createIndex(Customer.class, "index_first_name", "firstName", IndexType.STRING);
    }

    @Test
    public void testDeleteIndex() {
        repository.deleteIndex(Customer.class, "index_first_name");

        verify(operations).deleteIndex(Customer.class, "index_first_name");
    }
}
