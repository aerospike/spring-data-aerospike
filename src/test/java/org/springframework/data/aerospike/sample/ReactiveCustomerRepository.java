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

import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
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

    Mono<Customer> findOneByLastNameNot(String lastName);

    Mono<Collection<Customer>> findByFirstNameNotIgnoreCase(String lastName);

    Mono<Customer> findOneByLastName(String lastName);

    Flux<Customer> findByLastNameOrderByFirstNameAsc(String lastName);

    Flux<Customer> findByLastNameOrderByFirstNameDesc(String lastName);

    Flux<Customer> findByFirstNameEndsWith(String postfix);

    Flux<Customer> findByFirstNameStartsWithOrderByAgeAsc(String prefix);

    Flux<CustomerSomeFields> findCustomerSomeFieldsByFirstNameStartsWithOrderByFirstNameAsc(String prefix);

    Flux<Customer> findByAgeLessThan(long age, Sort sort);

    Flux<Customer> findByFirstNameIn(List<String> firstNames);

    Flux<Customer> findByFirstNameAndLastName(QueryParam firstName, QueryParam lastName);

    Mono<Customer> findOneByFirstNameAndLastName(QueryParam firstName, QueryParam lastName);

    Flux<Customer> findByLastNameAndAge(QueryParam lastName, QueryParam age);

    Mono<Long> countByIdAndFirstName(QueryParam ids, QueryParam firstNames);

    Mono<Void> deleteByIdAndFirstName(QueryParam ids, QueryParam firstNames);

    Mono<Boolean> existsByIdAndFirstName(QueryParam ids, QueryParam firstNames);

    Flux<Customer> findByAgeBetween(long from, long to);

    Mono<Long> countByAgeBetween(long from, long to);

    Flux<Customer> findByFirstNameContains(String firstName);

    Flux<Customer> findByFirstNameContainingIgnoreCase(String firstName);

    Flux<Customer> findByAgeBetweenAndLastName(QueryParam ageBetween, QueryParam lastName);

    Flux<Customer> findByAgeBetweenOrderByFirstNameDesc(long i, long j);

    Flux<Customer> findByGroup(char group);

    Flux<Customer> findByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Flux<Customer> readByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Flux<Customer> getByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Flux<Customer> queryByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Flux<Customer> searchByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Flux<Customer> streamByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Mono<Boolean> existsByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Mono<Long> countByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);

    Mono<Void> deleteByIdAndFirstNameIn(QueryParam ids, QueryParam firstNames);
}
