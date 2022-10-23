/*
 * Copyright 2012-2021 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 		https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * Simple reactive repository interface managing {@link Customer}s.
 *
 * @author Igor Ermolenko
 */
public interface ReactiveCustomerRepository extends ReactiveAerospikeRepository<Customer, String> {

    Flux<Customer> findByLastName(String lastName);

    // DTO Projection
    Flux<CustomerSomeFields> findCustomerSomeFieldsByLastName(String lastName);

    // Dynamic Projection
    <T> Flux<T> findByLastName(String lastName, Class<T> type);

    Flux<Customer> findByLastNameNot(String lastName);

    Mono<Customer> findOneByLastName(String lastName);

    Flux<Customer> findByLastNameOrderByFirstNameAsc(String lastName);

    Flux<Customer> findByLastNameOrderByFirstNameDesc(String lastName);

    Flux<Customer> findByFirstNameEndsWith(String postfix);

    Flux<Customer> findByFirstNameStartsWithOrderByAgeAsc(String prefix);

    Flux<CustomerSomeFields> findCustomerSomeFieldsByFirstNameStartsWithOrderByFirstNameAsc(String prefix);

    Flux<Customer> findByAgeLessThan(long age, Sort sort);

    Flux<Customer> findByFirstNameIn(List<String> firstNames);

    Flux<Customer> findByFirstNameAndLastName(String firstName, String lastName);

    Mono<Customer> findOneByFirstNameAndLastName(String firstName, String lastName);

    Flux<Customer> findByLastNameAndAge(String lastName, long age);

    Flux<Customer> findByAgeBetween(long from, long to);

    Flux<Customer> findByFirstNameContains(String firstName);

    Flux<Customer> findByFirstNameContainingIgnoreCase(String firstName);

    Flux<Customer> findByAgeBetweenAndLastName(long from, long to, String lastName);

    Flux<Customer> findByAgeBetweenOrderByFirstNameDesc(long i, long j);

    Flux<Customer> findByGroup(char group);
}
