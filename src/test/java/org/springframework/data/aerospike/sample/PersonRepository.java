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

import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public interface PersonRepository<P extends Person> extends AerospikeRepository<P, String> {

	List<P> findByLastName(String lastName);
	
	Page<P> findByLastNameStartsWithOrderByAgeAsc(String prefix, Pageable pageable);

	List<P> findByLastNameEndsWith(String postfix);

	List<P> findByLastNameOrderByFirstNameAsc(String lastName);
	
	List<P> findByLastNameOrderByFirstNameDesc(String lastName);

	List<P> findByFirstNameLike(String firstName);

	List<P> findByFirstNameLikeOrderByLastNameAsc(String firstName, Sort sort);

	List<P> findByAgeLessThan(int age, Sort sort);

	Stream<P> findByFirstNameIn(List<String> firstNames);

	Stream<P> findByFirstNameNotIn(Collection<String> firstNames);

	List<P> findByFirstNameAndLastName(String firstName, String lastName);

	List<P> findByAgeBetween(int from, int to);

	@SuppressWarnings("rawtypes")
	Person findByShippingAddresses(Set address);

	List<P> findByAddress(Address address);

	List<P> findByAddressZipCode(String zipCode);

	List<P> findByLastNameLikeAndAgeBetween(String lastName, int from, int to);

	List<P> findByAgeOrLastNameLikeAndFirstNameLike(int age, String lastName, String firstName);

//	List<P> findByNamedQuery(String firstName);

	List<P> findByCreator(User user);

	List<P> findByCreatedAtLessThan(Date date);

	List<P> findByCreatedAtGreaterThan(Date date);

//	List<P> findByCreatedAtLessThanManually(Date date);

	List<P> findByCreatedAtBefore(Date date);

	List<P> findByCreatedAtAfter(Date date);

	Stream<P> findByLastNameNot(String lastName);

	List<P> findByCredentials(Credentials credentials);
	
	List<P> findCustomerByAgeBetween(Integer from, Integer to);

	List<P> findByAgeIn(ArrayList<Integer> ages);

	List<P> findPersonByFirstName(String firstName);

	long countByLastName(String lastName);

	int countByFirstName(String firstName);

	long someCountQuery(String lastName);

	List<P> findByFirstNameIgnoreCase(String firstName);

	List<P> findByFirstNameNotIgnoreCase(String firstName);

	List<P> findByFirstNameStartingWithIgnoreCase(String firstName);

	List<P> findByFirstNameEndingWithIgnoreCase(String firstName);

	List<P> findByFirstNameContainingIgnoreCase(String firstName);

	Slice<P> findByAgeGreaterThan(int age, Pageable pageable);

	List<P> deleteByLastName(String lastName);

	Long deletePersonByLastName(String lastName);

	Page<P> findByAddressIn(List<Address> address, Pageable page);

	List<P> findByMapKeysContaining(String key);

	List<P> findByStringsContaining(String string);

	List<P> findByIntsContaining(Integer integer);

	List<P> findByIntsContaining(List integer);

	List<P> findTop3ByLastNameStartingWith(String lastName);

	Page<P> findTop3ByLastNameStartingWith(String lastName, Pageable pageRequest);

	List<P> findByFirstName(String string);

	List<P> findByFirstNameAndAge(String string, int i);

	Iterable<P> findByAgeBetweenAndLastName(int from, int to, String lastName);

	List<P> findByFirstNameStartsWith(String string);

	Iterable<P> findByAgeBetweenOrderByLastName(int i, int j);

}
