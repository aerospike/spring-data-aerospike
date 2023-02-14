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
package org.springframework.data.aerospike.repository;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.CustomerRepository;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Oliver Gierke
 */
public class CustomerRepositoriesIntegrationTests extends BaseBlockingIntegrationTests {

    @Autowired
    CustomerRepository repository;

    @Test
    public void create() {
        repository.save(Customer.builder().id(id).firstName("Dave").lastName("Matthews").build());
    }

    @Test
    public void exists() {
        repository.save(Customer.builder().id(id).firstName("Dave").lastName("Matthews").build());

        boolean exists = repository.existsById(id);

        assertThat(exists).isTrue();
    }

    @Test
    public void delete() {
        repository.delete(Customer.builder().id(id).firstName("Dave").lastName("Matthews").group('a').build());
    }

    @Test
    public void readById() {
        Customer customer = repository.save(Customer.builder()
            .id(id)
            .firstName("Dave")
            .lastName("Matthews")
            .group('a')
            .build());

        Optional<Customer> findById = repository.findById(id);

        assertThat(findById).hasValueSatisfying(actual -> {
            assertThat(actual.getLastName()).isEqualTo(customer.getLastName());
            assertThat(actual.getFirstName()).isEqualTo(customer.getFirstName());
            assertThat(actual.getGroup()).isEqualTo(customer.getGroup());
        });
    }

    @Test
    public void findAllByIDs() {
        Customer first = repository.save(Customer.builder().id(nextId()).firstName("Dave").lastName("AMatthews")
            .build());
        Customer second = repository.save(Customer.builder().id(nextId()).firstName("Dave").lastName("BMatthews")
            .build());
        repository.save(Customer.builder().id(nextId()).firstName("Dave").lastName("CMatthews").build());
        repository.save(Customer.builder().id(nextId()).firstName("Dave").lastName("DMatthews").build());

        Iterable<Customer> customers = repository.findAllById(Arrays.asList(first.getId(), second.getId()));

        assertThat(customers).hasSize(2);
    }

    @Test
    public void findByGroup() {
        Customer first = repository.save(Customer.builder()
            .id(nextId())
            .firstName("Dave")
            .lastName("AMatthews")
            .group('c')
            .build());
        Customer second = repository.save(Customer.builder()
            .id(nextId())
            .firstName("Dave")
            .lastName("BMatthews")
            .group('d')
            .build());
        Customer third = repository.save(Customer.builder()
            .id(nextId())
            .firstName("Dave")
            .lastName("CMatthews")
            .group('d')
            .build());

        List<Customer> results = repository.findByGroup('d');
        assertThat(results).hasSize(2).containsOnly(second, third);

        results = repository.findByGroup('c');
        assertThat(results).hasSize(1).containsOnly(first);

        results = repository.findByGroup('e');
        assertThat(results).isEmpty();
    }
}
