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

    Flux<Customer> findByLastname(String lastname);

    Flux<Customer> findByLastnameNot(String lastname);

    Mono<Customer> findOneByLastname(String lastname);

    Flux<Customer> findByLastnameOrderByFirstnameAsc(String lastname);

    Flux<Customer> findByLastnameOrderByFirstnameDesc(String lastname);

    Flux<Customer> findByFirstnameEndsWith(String postfix);

    Flux<Customer> findByFirstnameStartsWithOrderByAgeAsc(String prefix);

    Flux<Customer> findByAgeLessThan(long age, Sort sort);

    Flux<Customer> findByFirstnameIn(List<String> firstnames);

    Flux<Customer> findByFirstnameAndLastname(String firstname, String lastname);

    Mono<Customer> findOneByFirstnameAndLastname(String firstname, String lastname);

    Flux<Customer> findByLastnameAndAge(String lastname, long age);

    Flux<Customer> findByAgeBetween(long from, long to);

    Flux<Customer> findByFirstnameContains(String firstname);

    Flux<Customer> findByFirstnameContainingIgnoreCase(String firstname);

    Flux<Customer> findByAgeBetweenAndLastname(long from, long to, String lastname);

    Flux<Customer> findByAgeBetweenOrderByFirstnameDesc(long i, long j);
}
