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

import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
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
 * @author Peter Milne
 * @author Jean Mercier
 */
public interface PersonRepository<P extends Person> extends AerospikeRepository<P, String> {

	List<P> findByLastName(String lastName);

	// DTO Projection
	List<PersonSomeFields> findPersonSomeFieldsByLastName(String lastName);

	// Dynamic Projection
	<T> List<T> findByLastName(String lastName, Class<T> type);
	
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

	List<P> findByFriendAgeBetween(int from, int to);

	@SuppressWarnings("rawtypes")
	Person findByShippingAddresses(Set address);

	List<P> findByAddress(Address address);

	List<P> findByAddressZipCode(String zipCode);

	List<P> findByAddressZipCodeContaining(String str);

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

	// DTO Projection
	Slice<PersonSomeFields> findPersonSomeFieldsByAgeGreaterThan(int age, Pageable pageable);

	List<P> deleteByLastName(String lastName);

	Long deletePersonByLastName(String lastName);

	Page<P> findByAddressIn(List<Address> address, Pageable page);

	/**
	 * Find all entities containing the given map element (key or value depending on the given criteria)
	 * @param element  map element
	 * @param criteria  KEY or VALUE
	 */
	List<P> findByStringMapContaining(String element, CriteriaDefinition.AerospikeMapCriteria criteria);

	/**
	 * Find all entities that satisfy the condition "have exactly the given map key and the given value"
	 * @param key Map key
	 * @param value Value of the key
	 */
	List<P> findByStringMapEquals(String key, String value);

	/**
	 * Find all entities that satisfy the condition "have the given map key and NOT the given value"
	 * @param key Map key
	 * @param value Value of the key
	 */
	List<P> findByIntMapIsNot(String key, Integer value);

	/**
	 * Find all entities that satisfy the condition "have the given map key and a value that starts with the given string"
	 * @param key Map key
	 * @param valueStartsWith String to check if value starts with it
	 */
	List<P> findByStringMapStartsWith(String key, String valueStartsWith);

	/**
	 * Find all entities that satisfy the condition "have the given map key and a value that contains the given string"
	 * @param key Map key
	 * @param valuePart String to check if value contains it
	 */
	List<P> findByStringMapContaining(String key, String valuePart);

	/**
	 * Find all entities that satisfy the condition "have the given map key and a value that is greater than the given integer"
	 * @param key Map key
	 * @param greaterThan Integer to check if value is greater than it
	 */
	List<P> findByIntMapGreaterThan(String key, Integer greaterThan);

	/**
	 * Find all entities that satisfy the condition "have the given map key and a value in between the given integers"
	 * @param key Map key
	 * @param from the lower limit for the map value, inclusive
	 * @param to the upper limit for the map value, inclusive
	 */
	List<P> findByIntMapBetween(String key, Integer from, Integer to);

	List<P> findByFriendLastName(String value);

	List<P> findByFriendAge(int value);

	List<P> findByFriendAgeGreaterThan(int value);

	List<P> findByFriendAgeGreaterThanEqual(int value);

	List<P> findByStringsContaining(String string);

	List<P> findByIntsContaining(Integer integer);

	List<P> findByIntsGreaterThan(Integer integer);

	List<P> findByIntsLessThanEqual(Integer integer);

	List<P> findByIntsContaining(List<Integer> integer);

	List<P> findTop3ByLastNameStartingWith(String lastName);

	Page<P> findTop3ByLastNameStartingWith(String lastName, Pageable pageRequest);

	List<P> findByFirstName(String string);

	List<P> findByFirstNameAndAge(String string, int i);

	Iterable<P> findByAgeBetweenAndLastName(int from, int to, String lastName);

	List<P> findByFirstNameStartsWith(String string);

	List<P> findByFriendFirstNameStartsWith(String string);

	Iterable<P> findByAgeBetweenOrderByLastName(int i, int j);
}
