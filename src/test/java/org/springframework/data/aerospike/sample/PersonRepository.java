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
package org.springframework.data.aerospike.sample;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface PersonRepository extends AerospikeRepository<Person, String> {

	List<Person> findByLastName(String lastName);
	
	Page<Person> findByLastNameStartsWithOrderByAgeAsc(String prefix, Pageable pageable);

	List<Person> findByLastNameEndsWith(String postfix);

	List<Person> findByLastNameOrderByFirstNameAsc(String lastName);
	
	List<Person> findByLastNameOrderByFirstNameDesc(String lastName);

	List<Person> findByFirstNameLike(String firstName);

	List<Person> findByFirstNameLikeOrderByLastNameAsc(String firstName, Sort sort);

	List<Person> findByAgeLessThan(int age, Sort sort);

	Stream<Person> findByFirstNameIn(List<String> firstNames);

	Stream<Person> findByFirstNameNotIn(Collection<String> firstNames);

	List<Person> findByFirstNameAndLastName(String firstName, String lastName);

	List<Person> findByAgeBetween(int from, int to);

	@SuppressWarnings("rawtypes")
	Person findByShippingAddresses(Set address);

	List<Person> findByAddress(Address address);

	List<Person> findByAddressZipCode(String zipCode);

	List<Person> findByLastNameLikeAndAgeBetween(String lastName, int from, int to);

	List<Person> findByAgeOrLastNameLikeAndFirstNameLike(int age, String lastName, String firstName);

//	List<Person> findByNamedQuery(String firstName);

	List<Person> findByCreator(User user);

	List<Person> findByCreatedAtLessThan(Date date);

	List<Person> findByCreatedAtGreaterThan(Date date);

//	List<Person> findByCreatedAtLessThanManually(Date date);

	List<Person> findByCreatedAtBefore(Date date);

	List<Person> findByCreatedAtAfter(Date date);

	Stream<Person> findByLastNameNot(String lastName);

	List<Person> findByCredentials(Credentials credentials);
	
	List<Person> findCustomerByAgeBetween(Integer from, Integer to);

	List<Person> findByAgeIn(ArrayList<Integer> ages);

	List<Person> findPersonByFirstName(String firstName);

	long countByLastName(String lastName);

	int countByFirstName(String firstName);

	long someCountQuery(String lastName);

	List<Person> findByFirstNameIgnoreCase(String firstName);

	List<Person> findByFirstNameNotIgnoreCase(String firstName);

	List<Person> findByFirstNameStartingWithIgnoreCase(String firstName);

	List<Person> findByFirstNameEndingWithIgnoreCase(String firstName);

	List<Person> findByFirstNameContainingIgnoreCase(String firstName);

	Slice<Person> findByAgeGreaterThan(int age, Pageable pageable);

	List<Person> deleteByLastName(String lastName);

	Long deletePersonByLastName(String lastName);

	Page<Person> findByAddressIn(List<Address> address, Pageable page);

	List<Person> findTop3ByLastNameStartingWith(String lastName);

	Page<Person> findTop3ByLastNameStartingWith(String lastName, Pageable pageRequest);

	List<Person> findByFirstName(String string);

	List<Person> findByFirstNameAndAge(String string, int i);

	Iterable<Person> findByAgeBetweenAndLastName(int from, int to, String lastName);

	List<Person> findByFirstNameStartsWith(String string);

	Iterable<Person> findByAgeBetweenOrderByLastName(int i, int j);

}
