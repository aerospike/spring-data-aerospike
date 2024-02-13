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

import jakarta.validation.constraints.NotNull;
import org.springframework.data.aerospike.query.CombinedQueryParam;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapQueryCriteria;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriteria;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public interface PersonRepository<P extends Person> extends AerospikeRepository<P, String> {

    List<P> findByLastName(String lastName);

    List<P> findByEmailAddress(String email);

    // DTO Projection
    List<PersonSomeFields> findPersonSomeFieldsByLastName(String lastName);

    // DTO Projection
    List<PersonSomeFields> findPersonSomeFieldsById(String id);

    // Dynamic Projection
    <T> List<T> findByLastName(String lastName, Class<T> type);

    // Dynamic Projection
    <T> List<T> findById(String id, Class<T> type);

    // Dynamic Projection
    <T> List<T> findByIdAndLastName(CombinedQueryParam ids, CombinedQueryParam lastName, Class<T> type);

    // Dynamic Projection
    <T> List<T> findByLastNameAndId(CombinedQueryParam lastName, CombinedQueryParam id, Class<T> type);

    // Dynamic Projection
    <T> List<T> findByFirstNameAndLastName(CombinedQueryParam firstName, CombinedQueryParam lastName, Class<T> type);

    /**
     * Find all entities that satisfy the condition "have primary key in the given list and first name equal to the
     * specified string".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare with
     */
    List<P> findByIdAndFirstName(CombinedQueryParam ids, CombinedQueryParam firstName);

    /**
     * Find all entities that satisfy the condition "have primary key in the given list and either first name equal to
     * the specified string or age equal to the specified integer".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare firstName with
     * @param age       integer to compare age with
     */
    List<P> findByIdAndFirstNameAndAge(CombinedQueryParam ids, CombinedQueryParam firstName, CombinedQueryParam age);

    List<P> findByIdAndFirstNameOrAge(CombinedQueryParam ids, CombinedQueryParam firstName, CombinedQueryParam age);

    Page<P> findByLastNameStartsWithOrderByAgeAsc(String prefix, Pageable pageable);

    List<P> findByLastNameEndsWith(String postfix);

    List<P> findByLastNameOrderByFirstNameAsc(String lastName);

    List<P> findByLastNameOrderByFirstNameDesc(String lastName);

    /**
     * Find all entities with firstName matching the given regex. POSIX Extended Regular Expression syntax is used to
     * interpret the regex.
     *
     * @param firstNameRegex Regex to find matching firstName
     */
    List<P> findByFirstNameLike(String firstNameRegex);

    List<P> findByFirstNameLikeIgnoreCase(String firstNameRegex);

    List<P> findByFirstNameLikeOrderByLastNameAsc(String firstName, Sort sort);

    /**
     * Find all entities with firstName matching the given regex. POSIX Extended Regular Expression syntax is used to
     * interpret the regex. The same as {@link #findByFirstNameLike(String)}
     *
     * @param firstNameRegex Regex to find matching firstName
     */
    List<P> findByFirstNameMatchesRegex(String firstNameRegex);

    List<P> findByFirstNameMatches(String firstNameRegex);

    List<P> findByFirstNameRegex(String firstNameRegex);

    List<P> findByFirstNameMatchesRegexIgnoreCase(String firstNameRegex);

    /**
     * Find all entities with age less than the given numeric parameter
     *
     * @param age  integer to compare with
     * @param sort sorting
     */
    List<P> findByAgeLessThan(int age, Sort sort);

    /**
     * Find all entities with age less than the given numeric parameter
     *
     * @param age  long to compare with, [Long.MIN_VALUE+1..Long.MAX_VALUE]
     * @param sort sorting
     */
    List<P> findByAgeLessThan(long age, Sort sort);

    Stream<P> findByFirstNameIn(List<String> firstNames);

    Stream<P> findByFirstNameNotIn(Collection<String> firstNames);

    List<P> findByAgeBigInteger(BigInteger age);

    List<P> findByAgeBigDecimal(BigDecimal age);

    /**
     * Find all entities that satisfy the condition "have age in the given range"
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    List<P> findByAgeBetween(int from, int to);

    /**
     * Find all entities that satisfy the condition "have the first name in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    List<P> findByFirstNameBetween(String from, String to);

    /**
     * Find all entities that satisfy the condition "have address in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>
     *
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    List<P> findByAddressBetween(Address from, Address to);

    /**
     * Find all entities that satisfy the condition "have a friend equal to the given argument" (find by POJO)
     *
     * @param friend - Friend to check for equality
     */
    List<P> findByFriend(Person friend);

    /**
     * Find all entities that satisfy the condition "have address equal to the given argument" (find by POJO)
     *
     * @param address - Address to check for equality
     */
    List<P> findByAddress(Address address);

    /**
     * Find all entities that satisfy the condition "have existing address not equal to the given argument" (find by
     * POJO)
     *
     * @param address - Address to compare with
     */
    List<P> findByAddressIsNot(Address address);

    List<P> findByAddressExists();

    List<P> findByAddressZipCodeExists();

    List<P> findByAddressIsNotNull();

    List<P> findByAddressIsNull();

    List<P> findByAddressZipCodeIsNull();

    /**
     * Find all entities that satisfy the condition "have a friend who has bestFriend with the address with zipCode
     * which is not null" (find by nested POJO field)
     */
    List<P> findByFriendBestFriendAddressZipCodeIsNull();

    /**
     * Find all entities that satisfy the condition "have address with existing zipCode"
     */
    List<P> findByAddressZipCodeIsNotNull();

    /**
     * Find all entities that satisfy the condition "have Address with fewer elements or with a corresponding key-value
     * lower in ordering than in the given argument" (find by POJO).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>
     *
     * @param address - Address to compare with
     */
    List<P> findByAddressLessThan(Address address);

    List<P> findByAddressZipCode(@NotNull String zipCode);

    List<P> findByAddressZipCodeContaining(String str);

    List<P> findByAddressZipCodeNotContaining(String str);

    List<P> findByFirstNameContaining(String str);

    List<P> findByFirstNameNotContaining(String str);

    List<P> findByLastNameLikeAndAgeBetween(CombinedQueryParam lastName, CombinedQueryParam ageBetween);

    List<P> findByAgeOrLastNameLikeAndFirstNameLike(CombinedQueryParam age, CombinedQueryParam lastName,
                                                    CombinedQueryParam firstName);

//	List<P> findByNamedQuery(String firstName);

    List<P> findByCreator(User user);

    List<P> findByCreatedAtLessThan(Date date);

    List<P> findByCreatedAtGreaterThan(Date date);

//	List<P> findByCreatedAtLessThanManually(Date date);

    List<P> findByDateOfBirthBefore(Date date);

    List<P> findByDateOfBirthAfter(Date date);

    List<P> findByRegDate(LocalDate date);

    List<P> findByRegDateBefore(LocalDate date);

    List<P> findByCreatedAtAfter(Date date);

    Stream<P> findByLastNameNot(String lastName);

    List<P> findByCredentials(Credentials credentials);

    List<P> findCustomerByAgeBetween(int from, int to);

    List<P> findByAgeIn(ArrayList<Integer> ages);

    List<P> findByIsActive(boolean isActive);

    List<P> findByIsActiveTrue();

    List<P> findByIsActiveFalse();

    List<P> findByIsActiveAndFirstName(CombinedQueryParam isActive, CombinedQueryParam firstName);

    @SuppressWarnings("UnusedReturnValue")
    long countByLastName(String lastName);

    int countByFirstName(String firstName);

    long someCountQuery(String lastName);

    List<P> findByFirstNameIgnoreCase(String firstName);

    List<P> findByFirstNameNotIgnoreCase(String firstName);

    List<P> findByFirstNameStartingWithIgnoreCase(String string);

    List<P> findDistinctByFirstNameStartingWith(String string);

    List<P> findDistinctByFirstNameContaining(String string);

    List<P> findByFirstNameEndingWithIgnoreCase(String string);

    List<P> findByFirstNameContainingIgnoreCase(String string);

    /**
     * Find all entities with age greater than the given numeric parameter
     *
     * @param age      integer to compare with
     * @param pageable Pageable
     */
    Slice<P> findByAgeGreaterThan(int age, Pageable pageable);

    /**
     * Find all entities with age less than the given numeric parameter
     *
     * @param age      integer to compare with
     * @param pageable Pageable
     */
    Page<P> findByAgeLessThan(int age, Pageable pageable);

    /**
     * Find all entities with age greater than the given numeric parameter
     *
     * @param age      long to compare with, [Long.MIN_VALUE..Long.MAX_VALUE-1]
     * @param pageable Pageable
     */
    Slice<P> findByAgeGreaterThan(long age, Pageable pageable);

    // DTO Projection
    Slice<PersonSomeFields> findPersonSomeFieldsByAgeGreaterThan(int age, Pageable pageable);

    List<P> deleteByLastName(String lastName);

    Long deletePersonByLastName(String lastName);

    Page<P> findByAddressIn(List<Address> address, Pageable page);

    /**
     * Find all entities that satisfy the condition "have strings the same as the given argument" (find by list)
     *
     * @param list List to compare strings with
     */
    List<P> findByStringsEquals(List<String> list);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is greater than the given
     * String"
     * <p>
     * List name in this case is Strings
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param string lower limit, exclusive
     */
    List<P> findByStringsGreaterThan(String string);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is less than or equal to the
     * given String"
     * <p>
     * List name in this case is Strings
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param string lower limit, inclusive
     */
    List<P> findByStringsLessThanEqual(String string);

    /**
     * Find all entities that satisfy the condition "have strings the same as the given argument" (find by collection)
     *
     * @param collection Collection to compare strings with
     */
    List<P> findByStrings(Collection<String> collection);

    /**
     * Find all entities with existing strings list not equal to the given argument
     *
     * @param list List to compare strings list with
     */
    List<P> findByStringsIsNot(List<String> list);

    /**
     * Find all entities that satisfy the condition "have strings list with fewer elements or with a corresponding
     * element lower in ordering than in the given argument" (find by list).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param list - List to compare with
     */
    List<P> findByStringsLessThan(List<String> list);

    /**
     * Find all entities that satisfy the condition "have integers list with more elements or with a corresponding
     * element higher in ordering than in the given argument" (find by list).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>
     *
     * @param list - List to compare with
     */
    List<P> findByIntsGreaterThan(List<Integer> list);

    /**
     * Find all entities that satisfy the condition "have strings set with more elements or with a corresponding element
     * higher in ordering than in the given argument" (find by collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering">Information about ordering</a>
     *
     * @param collection - Collection to compare with
     */
    List<P> findByIntSetGreaterThanEqual(Collection<Integer> collection);

    /**
     * Find all entities containing the given map element (key or value depending on the given criterion)
     *
     * @param element   map value
     * @param criterion {@link AerospikeMapQueryCriteria#KEY} or
     *                  {@link AerospikeMapQueryCriteria#VALUE}
     */
    List<P> findByStringMapContaining(String element, AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities that do not contain the given map element (key or value depending on the given criterion)
     *
     * @param element   map value
     * @param criterion {@link AerospikeMapQueryCriteria#KEY} or
     *                  {@link AerospikeMapQueryCriteria#VALUE}
     */
    List<P> findByStringMapNotContaining(String element, AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities that do not contain the given map element (key or value depending on the given criterion)
     *
     * @param nullCriterion {@link AerospikeNullQueryCriteria#NULL}
     * @param criterion {@link AerospikeMapQueryCriteria#KEY} or
     *                  {@link AerospikeMapQueryCriteria#VALUE}
     */
    List<P> findByStringMapNotContaining(AerospikeNullQueryCriteria nullCriterion, AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities containing the given map element (key or value depending on the given criterion)
     *
     * @param nullCriterion   {@link AerospikeNullQueryCriteria#NULL}
     * @param criterion {@link AerospikeMapQueryCriteria#KEY} or
     *                  {@link AerospikeMapQueryCriteria#VALUE}
     */
    List<P> findByStringMapContaining(AerospikeNullQueryCriteria nullCriterion, AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value equal to the given string"
     *
     * @param key   Map key
     * @param value String to check whether map value is not equal to it
     */
    List<P> findByStringMapNotContaining(String key, @NotNull String value);

    /**
     * Find all entities containing the given map element (key or value depending on the given criterion)
     *
     * @param value     map value
     * @param criterion {@link AerospikeMapQueryCriteria#KEY} or
     *                  {@link AerospikeMapQueryCriteria#VALUE}
     */
    List<P> findByMapOfIntListsContaining(List<Integer> value, AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities containing the given map value with the given key
     *
     * @param key   map key
     * @param value map value
     */
    List<P> findByMapOfIntListsContaining(String key, List<Integer> value);

    /**
     * Find all entities containing the given map value with the given key
     *
     * @param key   map key
     * @param value map value
     */
    List<P> findByAddressesMapContaining(String key, Address value);

    /**
     * Find all entities that satisfy the condition "have stringMap the same as the given argument" (find by map)
     *
     * @param map Map to compare stringMap with
     */
    List<P> findByStringMapEquals(Map<String, String> map);

    /**
     * Find all entities that satisfy the condition "have stringMap the same as the given argument" (find by map)
     *
     * @param map Map to compare stringMap with
     */
    List<P> findByStringMap(Map<String, String> map);

    /**
     * Find all entities that satisfy the condition "have stringMap with more elements or with a corresponding key-value
     * higher in ordering than in the given argument" (find by map).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param map - Map to compare with
     */
    List<P> findByStringMapGreaterThan(Map<String, String> map);

    /**
     * Find all entities that satisfy the condition "have exactly the given map key and the given value"
     *
     * @param key   Map key
     * @param value Value of the key
     */
    List<P> findByIntMapContaining(String key, int value);

    /**
     * Find all entities that satisfy the condition "have exactly the given map key and the given value"
     *
     * @param key1   Map key 1
     * @param value1 Value of key1
     * @param key2   Map key 2
     * @param value2 Value of key2
     */
    List<P> findByIntMapContaining(String key1, int value1, String key2, int value2);

    /**
     * Find all entities that satisfy the condition "have the map which contains the given boolean"
     *
     * @param key   Map key
     * @param value Boolean to check
     */
    List<P> findByMapOfBooleanContaining(String key, boolean value);

    /**
     * Find all entities with existing intMap not equal to the given argument
     *
     * @param map Map to compare intMap with
     */
    List<P> findByIntMapIsNot(Map<String, Integer> map);

    /**
     * Find all entities that satisfy the condition "have the given map key and NOT the given value"
     *
     * @param key   Map key
     * @param value Value of the key
     */
    List<P> findByIntMapIsNot(String key, int value);

    /**
     * Find all entities that satisfy the condition "have the given map key and NOT the given value"
     *
     * @param key   Map key
     * @param value Value of the key
     */
    List<P> findByMapOfIntListsIsNot(String key, List<Integer> value);

    /**
     * Find all entities that satisfy the condition "have the given map key and NOT the given value"
     *
     * @param key   Map key
     * @param value Value of the key
     */
    List<P> findByAddressesMapIsNot(String key, Address value);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value that starts with the given
     * string"
     *
     * @param key             Map key
     * @param valueStartsWith String to check if map value starts with it
     */
    List<P> findByStringMapStartsWith(String key, String valueStartsWith);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value matching the given regex"
     * POSIX Extended Regular Expression syntax is used to interpret the regex.
     *
     * @param key        Map key
     * @param valueRegex Regex to find matching map value
     */
    List<P> findByStringMapLike(String key, String valueRegex);

    List<P> findByStringMapMatchesRegex(String key, String valueRegex);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value equal to the given string"
     *
     * @param key   Map key
     * @param value String to check if map value equals it
     */
    List<P> findByStringMapContaining(String key, @NotNull String value);

    /**
     * Find all entities that satisfy the condition "have the given map key3 and the value3 equal to the given strings"
     *
     * @param key1   Map key for the first key-value pair
     * @param value1 String to check if map value equals it using key1
     * @param key2   Map key for the second key-value pair
     * @param value2 String to check if map value equals it using key2
     */
    List<P> findByStringMapContaining(String key1, String value1, String key2, String value2);

    /**
     * Find all entities that satisfy the condition "have the given map keys and the values equal to the given strings"
     *
     * @param key1   Map key for the first key-value pair
     * @param value1 String to check if map value equals it using key1
     * @param key2   Map key for the second key-value pair
     * @param value2 String to check if map value equals it using key2
     * @param key3   Map key for the third key-value pair
     * @param value3 String to check if map value equals it using key3
     */
    List<P> findByStringMapContaining(String key1, String value1, String key2, String value2,
                                      String key3, String value3);

    /**
     * Find all entities that satisfy the condition "contains the given map elements (keys or values depending on the
     * criterion)"
     *
     * @param element1  First key or value
     * @param element2  Second key or value
     * @param element3  Third key or value
     * @param criterion {@link AerospikeMapQueryCriteria#KEY} or
     *                  {@link AerospikeMapQueryCriteria#VALUE}
     */
    List<P> findByStringMapContaining(String element1, String element2, String element3,
                                      AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities that satisfy the condition "have the given map elements (keys or values depending on the
     * criterion)"
     *
     * @param element1  First key or value
     * @param element2  Second key or value
     * @param criterion {@link AerospikeMapQueryCriteria#KEY} or
     *                  {@link AerospikeMapQueryCriteria#VALUE}
     */
    List<P> findByStringMapContaining(String element1, String element2,
                                      AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value containing the given string"
     *
     * @param key1      Map key for the first key-value pair
     * @param value1    String to check if map value equals it using key1
     * @param key2      Map key for the second key-value pair
     * @param value2    String to check if map value equals it using key2
     * @param criterion {@link AerospikeMapQueryCriteria#VALUE_CONTAINING}
     */
    List<P> findByStringMapContaining(String key1, String value1, String key2, String value2,
                                      AerospikeMapQueryCriteria criterion);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value that is greater than the given
     * integer"
     *
     * @param key         Map key
     * @param greaterThan integer to check if map value is greater than it
     */
    List<P> findByIntMapGreaterThan(String key, int greaterThan);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value that is less than or equal to
     * the given integer"
     *
     * @param key               Map key
     * @param lessThanOrEqualTo integer to check if map value satisfies the condition
     */
    List<P> findByIntMapLessThanEqual(String key, int lessThanOrEqualTo);

    List<P> findByLongIntMapLessThanEqual(long key, int lessThanOrEqualTo);

    List<P> findByDoubleIntMapLessThanEqual(double key, int lessThanOrEqualTo);

    List<P> findByByteArrayIntMapLessThanEqual(byte[] key, int lessThanOrEqualTo);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value in the range between the given
     * integers"
     *
     * @param key  Map key
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    List<P> findByIntMapBetween(String key, int from, int to);

    /**
     * Find all entities that satisfy the condition "have the map in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    List<P> findByIntMapBetween(Map<String, Integer> from, Map<String, Integer> to);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value in the range between the given
     * strings"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param key  Map key
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    List<P> findByStringMapBetween(String key, String from, String to);

    /**
     * Find all entities that satisfy the condition "have a bestFriend who has a friend with the given map key and the
     * value in the range between the given integers (deeply nested)"
     *
     * @param key  Map key
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    List<P> findByBestFriendFriendIntMapBetween(String key, int from, int to);

    /**
     * Find all entities that satisfy the condition "have a bestFriend who has a friend with address apartment value in
     * the range between the given integers (deeply nested)"
     *
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    List<P> findByBestFriendFriendAddressApartmentBetween(int from, int to);

    List<P> findByFriendLastName(String value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age equal to the given integer" (find by
     * POJO field)
     *
     * @param value - number to check for equality
     */
    List<P> findByFriendAge(int value);

    /**
     * Find all entities that satisfy the condition "have a friend with the existing age NOT equal to the given integer"
     * (find by POJO field)
     *
     * @param value - number to check for inequality
     */
    List<P> findByFriendAgeIsNot(int value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age greater than the given integer" (find by
     * POJO field)
     *
     * @param value - lower limit, exclusive
     */
    List<P> findByFriendAgeGreaterThan(int value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age less than or equal to the given integer"
     * (find by POJO field)
     *
     * @param value - upper limit, inclusive
     */
    List<P> findByFriendAgeLessThanEqual(int value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age in the given range" (find by POJO
     * field)
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    List<P> findByFriendAgeBetween(int from, int to);

    /**
     * Find all entities that satisfy the condition "have a friend with the address equal to the given argument" (find
     * by inner POJO)
     *
     * @param address - Address to check for equality
     */
    List<P> findByFriendAddress(Address address);

    /**
     * Find all entities that satisfy the condition "have a friend with the address with zipCode equal to the given
     * argument" (find by nested POJO field)
     *
     * @param zipCode - Zip code to check for equality
     */
    List<P> findByFriendAddressZipCode(String zipCode);

    /**
     * Find all entities that satisfy the condition "have a friend who has bestFriend with the address with zipCode
     * equal to the given argument" (find by nested POJO field)
     *
     * @param zipCode - Zip code to check for equality
     */
    List<P> findByFriendBestFriendAddressZipCode(@NotNull String zipCode);

    /**
     * Find all entities that satisfy the condition "have a friend who has bestFriend with the address with apartment
     * equal to the given argument" (find by nested POJO field)
     *
     * @param apartment - Apartment number to check for equality
     */
    List<P> findByFriendBestFriendAddressApartment(Integer apartment);

    /**
     * Find all entities that satisfy the condition "have a friend who has a friend with the address with zipCode equal
     * to the given argument" (find by POJO field)
     *
     * @param zipCode - Zip code to check for equality
     */
    List<P> findByFriendFriendAddressZipCode(String zipCode);

    /**
     * Find all entities that satisfy the condition "have a friend who has a friend (etc.) ... who has the address with
     * zipCode equal to the given argument" (find by deeply nested POJO field)
     *
     * @param zipCode - Zip code to check for equality
     */
    List<P> findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendFriendAddressZipCode(String zipCode);

    /**
     * Find all entities that satisfy the condition "have a friend who has a friend (etc.) ... who has the address with
     * apartment number equal to the given argument" (find by deeply nested POJO field)
     *
     * @param apartment - Integer to check for equality
     */
    List<P> findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressApartment(Integer apartment);

    /**
     * Find all entities that satisfy the condition "have a friend who has a friend (etc.) ... who has the address equal
     * to the given argument" (find by deeply nested POJO)
     *
     * @param address - Address to check for equality
     */
    List<P> findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendBestFriendAddress(Address address);

    /**
     * Find all entities that satisfy the condition "have the list which contains the given string"
     * <p>
     * List name in this case is Strings
     * </p>
     *
     * @param string string to check
     */
    List<P> findByStringsContaining(String string);

    /**
     * Find all entities that satisfy the condition "have the list which contains null"
     * <p>
     * List name in this case is Strings
     * </p>
     *
     * @param nullCriterion {@link AerospikeNullQueryCriteria#NULL}
     */
    List<P> findByStringsContaining(AerospikeNullQueryCriteria nullCriterion);

    /**
     * Find all entities that satisfy the condition "have the list which does not contain the given string"
     * <p>
     * List name in this case is Strings
     * </p>
     *
     * @param string string to check
     */
    List<P> findByStringsNotContaining(String string);

    /**
     * Find all entities that satisfy the condition "have the list which does not contain the given string"
     * <p>
     * List name in this case is Strings
     * </p>
     *
     * @param nullCriteria {@link AerospikeNullQueryCriteria#NULL} to check for null
     */
    List<P> findByStringsNotContaining(AerospikeNullQueryCriteria nullCriteria);

    List<P> findByStringsNotContaining();

    /**
     * Find all entities that satisfy the condition "have the list which contains the given integer"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer number to check
     */
    List<P> findByIntsContaining(int integer);

    /**
     * Find all entities that satisfy the condition "have the list which contains the given integers"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer1 number to check
     * @param integer2 number to check
     */
    List<P> findByIntsContaining(int integer1, int integer2);

    /**
     * Find all entities that satisfy the condition "have the list which contains the given integers"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer1 number to check
     * @param integer2 number to check
     * @param integer3 number to check
     */
    List<P> findByIntsContaining(int integer1, int integer2, int integer3);

    /**
     * Find all entities that satisfy the condition "have the array which contains the given integer"
     * <p>
     * Array name in this case is IntArray
     * </p>
     *
     * @param integer number to check
     */
    List<P> findByIntArrayContaining(int integer);

    /**
     * Find all entities that satisfy the condition "have the array which contains the given integers"
     * <p>
     * Array name in this case is IntArray
     * </p>
     *
     * @param integer1 number to check
     * @param integer2 number to check
     */
    List<P> findByIntArrayContaining(int integer1, int integer2);

    /**
     * Find all entities that satisfy the condition "have the list which contains the given boolean"
     *
     * @param value boolean to check
     */
    List<P> findByListOfBooleanContaining(boolean value);

    /**
     * Find all entities that satisfy the condition "have list that contains the given Address".
     *
     * @param address Value to look for
     */
    List<P> findByAddressesListContaining(Address address);

    /**
     * Find all entities that satisfy the condition "have list that does not contain the given Address".
     *
     * @param address Value to look for
     */
    List<P> findByAddressesListNotContaining(Address address);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is greater than the given
     * integer"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer lower limit, exclusive
     */
    List<P> findByIntsGreaterThan(int integer);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is greater than the given long
     * value"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param value lower limit, exclusive, [Long.MIN_VALUE..Long.MAX_VALUE-1]
     */
    List<P> findByIntsGreaterThan(long value);

    /**
     * Find all entities that satisfy the condition "have the list of lists which is greater than the given list".
     * <p>
     * ListOfIntLists is the name of the list of lists
     * </p>
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param list List to compare with
     */
    List<P> findByListOfIntListsGreaterThan(List<List<Integer>> list);

    /**
     * Find all entities that satisfy the condition "have at least one array value which is greater than the given
     * integer"
     * <p>
     * Array name in this case is IntArray
     * </p>
     *
     * @param integer lower limit, exclusive, [Long.MIN_VALUE..Long.MAX_VALUE-1]
     */
    List<P> findByIntArrayGreaterThan(int integer);

    /**
     * Find all entities that satisfy the condition "have at least one array value which is greater than the given
     * integer"
     * <p>
     * Array name in this case is IntArray
     * </p>
     *
     * @param value lower limit, exclusive
     */
    List<P> findByIntArrayGreaterThan(long value);

    /**
     * Find all entities that satisfy the condition "have at least one map value (with the given key) which is greater
     * than the given parameter (list of integers)"
     * <p>
     * MapOfIntLists is a map of lists
     * </p>
     *
     * @param key   Map key
     * @param value Map value to compare with
     */
    List<P> findByMapOfIntListsGreaterThan(String key, List<Integer> value);

    /**
     * Find all entities that satisfy the condition "have map in the given range"
     * <p>
     * Map name in this case is MapOfIntLists
     * </p>
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    List<P> findByMapOfIntListsBetween(Map<String, List<Integer>> from, Map<String, List<Integer>> to);

    /**
     * Find all entities that satisfy the condition "have at least one map value with the given key which is greater
     * than the given Address".
     * <p>
     * Custom object such as Address is converted to a Map before being compared
     * </p>
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     * </p>
     *
     * @param key   Map key
     * @param value Map value to compare with
     */
    List<P> findByAddressesMapLessThan(String key, Address value);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is less than or equal to the
     * given integer"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer upper limit, inclusive
     */
    List<P> findByIntsLessThanEqual(int integer);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is less than or equal to the
     * given long value"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param value upper limit, inclusive, [Long.MIN_VALUE..Long.MAX_VALUE-1]
     */
    List<P> findByIntsLessThanEqual(long value);

    /**
     * Find all entities that satisfy the condition "have at least one list value in the given range"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    List<P> findByIntsBetween(int from, int to);

    /**
     * Find all entities that satisfy the condition "have list in the given range"
     * <p>
     * List name in this case is Ints
     * </p>
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    List<P> findByIntsBetween(List<Integer> from, List<Integer> to);

    /**
     * Find all entities that satisfy the condition "have list in the given range"
     * <p>
     * List name in this case is Strings
     * </p>
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    List<P> findByStringsBetween(List<String> from, List<String> to);

    /**
     * Find all entities that satisfy the condition "have at least one list value in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    List<P> findByStringsBetween(String from, String to);

    P findFirstByLastNameStartingWith(String lastName, Sort sort);

    List<P> findTopByLastNameStartingWith(String lastName, Sort sort);

    List<P> findTop3ByLastNameStartingWith(String lastName, Sort sort);

    List<P> findFirst3ByLastNameStartingWith(String lastName, Sort sort);

    Page<P> findTop3ByLastNameStartingWith(String lastName, Pageable pageRequest);

    List<P> findByFirstName(String name);

    List<P> findByFirstNameNot(String name);

    /**
     * Find all entities that satisfy the condition "have firstName higher in ordering than the given string".
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param string - String to compare with
     */
    List<P> findByFirstNameGreaterThan(String string);

    List<P> findByFirstNameAndAge(CombinedQueryParam string, CombinedQueryParam age);

    Iterable<P> findByAgeBetweenAndLastName(CombinedQueryParam ageBetween, CombinedQueryParam lastName);

    Iterable<P> findByAgeBetweenOrLastName(CombinedQueryParam ageBetween, CombinedQueryParam lastName);

    List<P> findByFirstNameStartsWith(String string);

    List<P> findByFriendFirstNameStartsWith(String string);

    /**
     * Distinct query for nested objects is currently not supported
     */
    List<P> findDistinctByFriendFirstNameStartsWith(String string);

    /**
     * Find all entities that satisfy the condition "have a friend with lastName matching the giving regex". POSIX
     * Extended Regular Expression syntax is used to interpret the regex.
     *
     * @param lastNameRegex Regex to find matching lastName
     */
    List<P> findByFriendLastNameLike(String lastNameRegex);

    List<P> findByFriendLastNameMatchesRegex(String lastNameRegex);

    /**
     * Find all entities that satisfy the condition "have age in the given range ordered by last name"
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    Iterable<P> findByAgeBetweenOrderByLastName(int from, int to);
}
