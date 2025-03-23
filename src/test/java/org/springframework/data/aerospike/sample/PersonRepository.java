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
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion;
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

    // DTO projection
    List<PersonId> findPersonIdByFirstName(String firstName);

    // DTO Projection
    List<PersonSomeFields> findPersonSomeFieldsById(String id);

    // Dynamic Projection
    <T> List<T> findByLastName(String lastName, Class<T> type);

    // Dynamic Projection
    <T> List<T> findById(String id, Class<T> type);

    // Dynamic Projection
    <T> List<T> findByIdAndLastName(QueryParam ids, QueryParam lastName, Class<T> type);

    // Dynamic Projection
    <T> List<T> findByLastNameAndId(QueryParam lastName, QueryParam id, Class<T> type);

    // Dynamic Projection
    <T> List<T> findByFirstNameAndLastName(QueryParam firstName, QueryParam lastName, Class<T> type);

    /**
     * Find all entities that satisfy the condition "have primary key in the given list and first name equal to the
     * specified string".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare with
     */
    List<P> findByIdAndFirstName(QueryParam ids, QueryParam firstName);

    /**
     * Find if there are entities that satisfy the condition "have primary key in the given list and first name equal to
     * the specified string".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare with
     */
    boolean existsByIdAndFirstName(QueryParam ids, QueryParam firstName);

    /**
     * Count all entities that satisfy the condition "have primary key in the given list and first name equal to the
     * specified string".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare with
     */
    long countByIdAndFirstName(QueryParam ids, QueryParam firstName);

    /**
     * Delete all entities that satisfy the condition "have primary key in the given list and first name equal to the
     * specified string".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare with
     */
    void deleteByIdAndFirstName(QueryParam ids, QueryParam firstName);

    /**
     * Find all entities that satisfy the condition "have primary key in the given list and first name equal to the
     * specified string".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare with
     */
    List<P> findAllByIdAndFirstName(QueryParam ids, QueryParam firstName);

    /**
     * Find all entities that satisfy the condition "have primary key in the given list and either first name equal to
     * the specified string or age equal to the specified integer".
     *
     * @param ids       List of primary keys
     * @param firstName String to compare firstName with
     * @param age       integer to compare age with
     */
    List<P> findByIdAndFirstNameAndAge(QueryParam ids, QueryParam firstName, QueryParam age);

    List<P> findByIdAndFirstNameOrAge(QueryParam ids, QueryParam firstName, QueryParam age);

    boolean existsByIdAndFirstNameOrAge(QueryParam ids, QueryParam firstName, QueryParam age);

    long countByIdAndFirstNameOrAge(QueryParam ids, QueryParam firstName, QueryParam age);

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

    List<P> findByIdLike(String idRegex);

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
     * Find if there are existing entities that satisfy the condition "have age in the given range"
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    boolean existsByAgeBetween(int from, int to);

    /**
     * Count existing entities that satisfy the condition "have age in the given range"
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    long countByAgeBetween(int from, int to);

    /**
     * Delete entities that satisfy the condition "have age in the given range"
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    void deleteByAgeBetween(int from, int to);

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
     * Find if there are existing entities  that satisfy the condition "have the first name in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    boolean existsByFirstNameBetween(String from, String to);

    /**
     * Count existing entities  that satisfy the condition "have the first name in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    long countByFirstNameBetween(String from, String to);

    /**
     * Delete entities  that satisfy the condition "have the first name in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param from lower limit for the map value, inclusive
     * @param to   upper limit for the map value, exclusive
     */
    void deleteByFirstNameBetween(String from, String to);

    List<P> findByIntsExists();

    List<P> findByIntMapExists();

    /**
     * Find all entities that satisfy the condition "have address in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
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

    List<P> findByAddressIsNotNull();

    List<P> findByAddressIsNull();

    /**
     * Find all entities that satisfy the condition "have Address with fewer elements or with a corresponding key-value
     * lower in ordering than in the given argument" (find by POJO).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param address - Address to compare with
     */
    List<P> findByAddressLessThan(Address address);

    List<P> findByFirstNameContaining(String str);

    List<P> findByFirstNameNotContaining(String str);

    List<P> findByLastNameLikeAndAgeBetween(QueryParam lastName, QueryParam ageBetween);

    List<P> findByAgeOrLastNameLikeAndFirstNameLike(QueryParam age, QueryParam lastName,
                                                    QueryParam firstName);

    List<P> findByCreator(User user);

    List<P> findByCreatedAtLessThan(Date date);

    List<P> findByCreatedAtGreaterThan(Date date);

    List<P> findByDateOfBirthBefore(Date date);

    List<P> findByDateOfBirthAfter(Date date);

    boolean existsByDateOfBirthAfter(Date date);

    long countByDateOfBirthAfter(Date date);

    void deleteByDateOfBirthAfter(Date date);

    List<P> findByRegDate(LocalDate date);

    List<P> findByRegDateBefore(LocalDate date);

    List<P> findByCreatedAtAfter(Date date);

    Stream<P> findByLastNameNot(String lastName);

    List<P> findByCredentials(Credentials credentials);

    List<P> findCustomerByAgeBetween(int from, int to);

    List<P> findByAgeIn(ArrayList<Integer> ages);

    List<P> findByIsActive(boolean isActive);

    List<P> findByIsActiveTrue();

    List<P> findByIsActiveIsTrue();

    List<P> findByIsActiveFalse();

    List<P> findByIsActiveAndFirstName(QueryParam isActive, QueryParam firstName);

    @SuppressWarnings("UnusedReturnValue")
    long countByLastName(String lastName);

    long someCountQuery(String lastName);

    List<P> findByFirstNameIgnoreCase(String firstName);

    long countByFirstNameIgnoreCase(String firstName);

    void deleteByFirstNameIgnoreCase(String firstName);

    List<P> findByFirstNameNotIgnoreCase(String firstName);

    List<P> findByFirstNameStartingWithIgnoreCase(String string);

    List<P> findDistinctByFirstNameStartingWith(String string);

    List<P> findDistinctByFirstNameContaining(String string);

    List<P> findByFirstNameEndingWithIgnoreCase(String string);

    List<P> findByFirstNameContainingIgnoreCase(String string);

    /**
     * Find all entities with age greater than the given numeric parameter
     *
     * @param age integer to compare with
     */
    List<P> findByAgeGreaterThan(int age);

    /**
     * Find all entities with age greater than the given numeric parameter
     *
     * @param age      integer to compare with
     * @param pageable Pageable
     */
    Slice<P> findByAgeGreaterThan(int age, Pageable pageable);

    /**
     * Delete entities with age greater than the given numeric parameter
     *
     * @param age      integer to compare with
     * @param pageable Pageable
     */
    Slice<Void> deleteByAgeGreaterThan(int age, Pageable pageable);

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
     * Find all entities that satisfy the condition "have strings the same as the given argument" (find by Collection)
     *
     * @param strings Collection to compare strings with, subsequently gets converted to a List
     */
    List<P> findByStringsEquals(Collection<String> strings);

    /**
     * Find all entities that satisfy the condition "have strings list equal to the given argument" (find by
     * Collection)
     *
     * @param strings Collection to compare strings with, subsequently gets converted to a List
     */
    List<P> findByStrings(Collection<String> strings);

    /**
     * Find all entities with existing strings list not equal to the given argument
     *
     * @param list List to compare strings list with
     */
    List<P> findByStringsIsNot(List<String> list);

    /**
     * Find all entities that satisfy the condition "have strings list with fewer elements or with a corresponding
     * element lower in ordering than in the given argument" (find by Collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param strings - Collection to compare with, subsequently gets converted to a List
     */
    List<P> findByStringsLessThan(Collection<String> strings);

    /**
     * Find all entities that satisfy the condition "have ints list with more elements or with a corresponding element
     * higher in ordering than in the given argument" (find by Collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param ints - Collection to compare with, subsequently gets converted to a List
     */
    List<P> findByIntsGreaterThan(Collection<Integer> ints);

    /**
     * Find all entities that satisfy the condition "have intSet with fewer elements or with a corresponding element
     * lower in ordering than in the given argument" (find by Collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param collection - Collection to compare with, subsequently gets converted to a List
     */
    List<P> findByIntSetLessThan(Collection<Integer> collection);

    /**
     * Find all entities that satisfy the condition "have intSet with fewer elements or with a corresponding element
     * lower in ordering than in the given argument, or equal to the given argument" (find by Collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param collection - Collection to compare with, subsequently gets converted to a List
     */
    List<P> findByIntSetLessThanEqual(Collection<Integer> collection);

    /**
     * Find all entities that satisfy the condition "have intSet with more elements or with a corresponding element
     * higher in ordering than in the given argument" (find by Collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param collection - Collection to compare with, subsequently gets converted to a List
     */
    List<P> findByIntSetGreaterThan(Collection<Integer> collection);

    /**
     * Find all entities that satisfy the condition "have intSet with more elements or with a corresponding element
     * higher in ordering than in the given argument, or equal to the given argument" (find by Collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param collection - Collection to compare with, subsequently gets converted to a List
     */
    List<P> findByIntSetGreaterThanEqual(Collection<Integer> collection);

    /**
     * Find all entities that satisfy the condition "have ints list with more elements or with a corresponding element
     * higher in ordering than in the given argument, or equal to the given argument" (find by Collection).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param collection - Collection to compare with, subsequently gets converted to a List
     */
    List<P> findByIntsGreaterThanEqual(Collection<Integer> collection);

    /**
     * Find all entities containing the given map element (key or value depending on the given criterion)
     *
     * @param criterion {@link AerospikeQueryCriterion#KEY} or {@link AerospikeQueryCriterion#VALUE}
     * @param element   map value
     */
    List<P> findByStringMapContaining(AerospikeQueryCriterion criterion, String element);

    /**
     * Find all entities containing the given map element (key or value depending on the given criterion)
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           map key
     * @param value         map value
     */
    List<P> findByStringMapContaining(AerospikeQueryCriterion criterionPair, String key, String value);

    /**
     * Find all entities that do not contain the given map element (key or value depending on the given criterion)
     *
     * @param element   map value
     * @param criterion {@link AerospikeQueryCriterion#KEY} or {@link AerospikeQueryCriterion#VALUE}
     */
    List<P> findByStringMapNotContaining(AerospikeQueryCriterion criterion, String element);

    /**
     * Find all entities that do not contain null element (key or value depending on the given criterion)
     *
     * @param criterion     {@link AerospikeQueryCriterion#KEY} or {@link AerospikeQueryCriterion#VALUE}
     * @param nullParameter {@link AerospikeNullQueryCriterion#NULL_PARAM}
     */
    List<P> findByStringMapNotContaining(AerospikeQueryCriterion criterion, AerospikeNullQueryCriterion nullParameter);

    /**
     * Find all entities containing the given map element (key or value depending on the given criterion)
     *
     * @param criterion     {@link AerospikeQueryCriterion#KEY} or {@link AerospikeQueryCriterion#VALUE}
     * @param nullParameter {@link AerospikeNullQueryCriterion#NULL_PARAM}
     */
    List<P> findByStringMapContaining(AerospikeQueryCriterion criterion, AerospikeNullQueryCriterion nullParameter);

    /**
     * Find all entities that satisfy the condition "does not have the given map key or does not have the given value"
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           Map key
     * @param value         String to check whether map value is not equal to it
     */
    List<P> findByStringMapNotContaining(AerospikeQueryCriterion criterionPair, String key, @NotNull String value);

    /**
     * Find all entities containing the given map element (key or value depending on the given criterion)
     *
     * @param criterion {@link AerospikeQueryCriterion#KEY} or {@link AerospikeQueryCriterion#VALUE}
     * @param value     map value
     */
    List<P> findByMapOfIntListsContaining(AerospikeQueryCriterion criterion, List<Integer> value);

    /**
     * Find all entities containing the given map value with the given key
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           map key
     * @param value         map value
     */
    List<P> findByMapOfIntListsContaining(AerospikeQueryCriterion criterionPair, String key, List<Integer> value);

    /**
     * Find all entities containing the given map value with the given key
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           map key
     * @param value         map value
     */
    List<P> findByAddressesMapContaining(AerospikeQueryCriterion criterionPair, String key, Address value);

    /**
     * Find all entities that satisfy the condition "have stringMap the same as the given argument" (find by Map)
     *
     * @param map Map to compare stringMap with
     */
    List<P> findByStringMapEquals(Map<String, String> map);

    /**
     * Find all entities that satisfy the condition "have stringMap the same as the given argument" (find by Map)
     *
     * @param map Map to compare stringMap with
     */
    List<P> findByStringMap(Map<String, String> map);

    /**
     * Find all entities that satisfy the condition "have stringMap with more elements or with a corresponding key-value
     * higher in ordering than in the given argument" (find by Map).
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param map - Map to compare with
     */
    List<P> findByStringMapGreaterThan(Map<String, String> map);

    /**
     * Find all entities that satisfy the condition "have the map which contains the given key and its boolean value".
     * Map name in this case is MapOfBoolean
     *
     * @param criterionPair criterion {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           Map key
     * @param value         Boolean value to check
     */
    List<P> findByMapOfBooleanContaining(AerospikeQueryCriterion criterionPair, String key, boolean value);

    /**
     * Find all entities with existing intMap not equal to the given argument
     *
     * @param map Map to compare intMap with
     */
    List<P> findByIntMapIsNot(Map<String, Integer> map);

    /**
     * Find all entities that satisfy the condition "have the given map key and the value equal to the given integer"
     *
     * @param criterionPair criterion {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           Map key
     * @param value         Integer to check if map value equals it
     */
    List<P> findByIntMapContaining(AerospikeQueryCriterion criterionPair, String key, int value);

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
     * Find all entities that satisfy the condition "have intMap equal to one of the values in the given list" (find by
     * Map)
     *
     * @param list - list of possible values
     */
    List<P> findByIntMapIn(List<Map<String, Integer>> list);

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
     * Find if there are entities that satisfy the condition "have a friend with the age in the given range" (find by
     * POJO field)
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    boolean existsByFriendAgeBetween(int from, int to);

    /**
     * Count entities that satisfy the condition "have a friend with the age in the given range" (find by POJO field)
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    long countByFriendAgeBetween(int from, int to);

    /**
     * Delete entities that satisfy the condition "have a friend with the age in the given range" (find by POJO field)
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, exclusive
     */
    void deleteByFriendAgeBetween(int from, int to);

    /**
     * Find all entities that satisfy the condition "have address with zipCode (find by nested simple property)"
     */

    List<P> findByAddressZipCodeExists();

    /**
     * Find all entities that satisfy the condition "have address with zipCode which is null (i.e. friend's first name
     * does not exist)" (find by nested simple property)
     */
    List<P> findByAddressZipCodeIsNull();

    /**
     * Find all entities that satisfy the condition "have a friend who has bestFriend with the address with zipCode
     * which is not null" (find by deeply nested simple property)
     */
    List<P> findByFriendBestFriendAddressZipCodeIsNull();

    /**
     * Find all entities that satisfy the condition "have address with zipCode which is not null (i.e. address's zipCode
     * exists)" (find by nested simple property)
     */
    List<P> findByAddressZipCodeIsNotNull();

    /**
     * Find all entities that satisfy the condition "have a friend with the ints list equal to the given argument" (find
     * by nested Collection)
     *
     * @param zipCode - String to check for equality
     */
    List<P> findByAddressZipCode(String zipCode);

    /**
     * Find all entities that satisfy the condition "have address with zipCode not equal to the given argument" (find by
     * nested simple property)
     *
     * @param zipCode - String to check for equality
     */
    List<P> findByAddressZipCodeIsNot(String zipCode);

    /**
     * Find all entities that satisfy the condition "have address with zipCode greater than or equal to the given
     * argument" (find by nested simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param zipCode - String to compare
     */
    List<P> findByAddressZipCodeGreaterThanEqual(String zipCode);

    /**
     * Find all entities that satisfy the condition "have address with zipCode greater than the given argument" (find by
     * nested simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param zipCode - String to compare
     */
    List<P> findByAddressZipCodeGreaterThan(String zipCode);

    /**
     * Find all entities that satisfy the condition "have address with zipCode less than or equal to the given argument"
     * (find by nested simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param zipCode - String to compare
     */
    List<P> findByAddressZipCodeLessThanEqual(String zipCode);

    /**
     * Find all entities that satisfy the condition "have address with zipCode less than the given argument" (find by
     * nested simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param zipCode - String to compare
     */
    List<P> findByAddressZipCodeLessThan(String zipCode);

    /**
     * Find all entities that satisfy the condition "have address with zipCode between the given arguments" (find by
     * nested simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param lowerLimit - lower limit, inclusive
     * @param upperLimit - upper limit, exclusive
     */
    List<P> findByAddressZipCodeBetween(String lowerLimit, String upperLimit);

    /**
     * Find if there are entities that satisfy the condition "have address with zipCode between the given arguments"
     * (find by nested simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param lowerLimit - lower limit, inclusive
     * @param upperLimit - upper limit, exclusive
     */
    boolean existsByAddressZipCodeBetween(String lowerLimit, String upperLimit);

    /**
     * Count entities that satisfy the condition "have address with zipCode between the given arguments" (find by nested
     * simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param lowerLimit - lower limit, inclusive
     * @param upperLimit - upper limit, exclusive
     */
    long countByAddressZipCodeBetween(String lowerLimit, String upperLimit);

    /**
     * Delete entities that satisfy the condition "have address with zipCode between the given arguments" (find by
     * nested simple property)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param lowerLimit - lower limit, inclusive
     * @param upperLimit - upper limit, exclusive
     */
    void deleteByAddressZipCodeBetween(String lowerLimit, String upperLimit);

    /**
     * Find all entities that satisfy the condition "have address with zipCode equal to one of the values in the given
     * list" (find by nested simple property)
     *
     * @param list - list of possible values
     */
    List<P> findByAddressZipCodeIn(List<String> list);

    /**
     * Find all entities that satisfy the condition "have address with zipCode equal to neither of the values in the
     * given list" (find by nested simple property)
     *
     * @param list - list of possible values
     */
    List<P> findByAddressZipCodeNotIn(List<String> list);

    /**
     * Find all entities that satisfy the condition "have address with zipCode that contains the given substring" (find
     * by nested simple property)
     *
     * @param string substring to check
     */
    List<P> findByAddressZipCodeContaining(String string);

    /**
     * Find all entities that satisfy the condition "have address with zipCode that does not contain the given string"
     * (find by nested simple property)
     *
     * @param string substring to check
     */
    List<P> findByAddressZipCodeNotContaining(String string);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list (find by nested Collection)"
     */
    List<P> findByFriendIntsExists();

    /**
     * Find all entities that satisfy the condition "have a friend with ints list which is null (i.e. friend's ints list
     * does not exist)" (find by nested Collection)
     */
    List<P> findByFriendIntsIsNull();

    /**
     * Find all entities that satisfy the condition "have a friend with ints list which is not null (i.e. friend's ints
     * list exists)" (find by nested Collection)
     */
    List<P> findByFriendIntsIsNotNull();

    /**
     * Find all entities that satisfy the condition "have a friend with ints list equal to the given argument" (find by
     * nested Collection)
     *
     * @param ints - List of integers to check for equality
     */
    List<P> findByFriendInts(List<Integer> ints);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list not equal to the given argument" (find
     * by nested Collection)
     *
     * @param ints - List of integers to check for equality
     */
    List<P> findByFriendIntsIsNot(List<Integer> ints);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list greater than or equal to the given
     * argument" (find by nested Collection)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param ints - List of integers to compare
     */
    List<P> findByFriendIntsGreaterThanEqual(List<Integer> ints);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list greater than the given argument" (find
     * by nested Collection)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param ints - List of integers to compare
     */
    List<P> findByFriendIntsGreaterThan(List<Integer> ints);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list less than or equal to the given
     * argument" (find by nested Collection)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param ints - List of integers to compare
     */
    List<P> findByFriendIntsLessThanEqual(List<Integer> ints);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list less than the given argument" (find by
     * nested Collection)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param ints - List of integers to compare
     */
    List<P> findByFriendIntsLessThan(List<Integer> ints);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list between the given arguments" (find by
     * nested Collection)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param lowerLimit - lower limit, inclusive
     * @param upperLimit - upper limit, exclusive
     */
    List<P> findByFriendIntsBetween(List<Integer> lowerLimit, List<Integer> upperLimit);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list equal to one of the values in the
     * given list" (find by nested Collection)
     *
     * @param list - list of possible values
     */
    List<P> findByFriendIntsIn(List<List<Integer>> list);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list equal to neither of the values in the
     * given list" (find by nested Collection)
     *
     * @param list - list of possible values
     */
    List<P> findByFriendIntsNotIn(List<List<Integer>> list);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list that contains the given integer" (find
     * by nested Collection)
     *
     * @param integer number to check
     */
    List<P> findByFriendIntsContaining(int integer);

    /**
     * Find all entities that satisfy the condition "have a friend with ints list that does not contain the given
     * integer" (find by nested Collection)
     *
     * @param integer number to check
     */
    List<P> findByFriendIntsNotContaining(int integer);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap (find by nested Map)"
     */
    List<P> findByFriendIntMapExists();

    /**
     * Find all entities that satisfy the condition "have a friend with intMap which is null (i.e. friend's intMap does
     * not exist)" (find by nested Map)
     */
    List<P> findByFriendIntMapIsNull();

    /**
     * Find all entities that satisfy the condition "have a friend with intMap which is not null (i.e. friend's intMap
     * exists)" (find by nested Map)
     */
    List<P> findByFriendIntMapIsNotNull();

    /**
     * Find all entities that satisfy the condition "have a friend with intMap equal to the given argument" (find by
     * nested Map)
     *
     * @param intMap - Map to check for equality
     */
    List<P> findByFriendIntMap(Map<String, Integer> intMap);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap not equal to the given argument" (find by
     * nested Map)
     *
     * @param intMap - Map to check for equality
     */
    List<P> findByFriendIntMapIsNot(Map<String, Integer> intMap);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap greater than or equal to the given
     * argument" (find by nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param intMap - Map to compare
     */
    List<P> findByFriendIntMapGreaterThanEqual(Map<String, Integer> intMap);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap greater than the given argument" (find by
     * nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param intMap - Map to compare
     */
    List<P> findByFriendIntMapGreaterThan(Map<String, Integer> intMap);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap less than or equal to the given argument"
     * (find by nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param intMap - Map to compare
     */
    List<P> findByFriendIntMapLessThanEqual(Map<String, Integer> intMap);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap less than the given argument" (find by
     * nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param intMap - Map to compare
     */
    List<P> findByFriendIntMapLessThan(Map<String, Integer> intMap);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap between the given arguments" (find by
     * nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param lowerLimit - lower limit, inclusive
     * @param upperLimit - upper limit, exclusive
     */
    List<P> findByFriendIntMapBetween(Map<String, Integer> lowerLimit, Map<String, Integer> upperLimit);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap equal to one of the values in the given
     * list" (find by nested Map)
     *
     * @param list - list of possible values
     */
    List<P> findByFriendIntMapIn(List<Map<String, Integer>> list);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap equal to neither of the values in the
     * given list" (find by nested Map)
     *
     * @param list - list of possible values
     */
    List<P> findByFriendIntMapNotIn(List<Map<String, Integer>> list);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap that contains the given integer" (find by
     * nested Map)
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           Map key
     * @param value         Integer to check whether map value equals it
     */
    List<P> findByFriendIntMapContaining(AerospikeQueryCriterion criterionPair, String key, @NotNull Integer value);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap that contains the given value - key or
     * value depending on the criterion parameter" (find by nested Map)
     *
     * @param criterion {@link AerospikeQueryCriterion#KEY or AerospikeQueryCriterion#VALUE}
     * @param element   Map key or value
     */
    List<P> findByFriendStringMapContaining(AerospikeQueryCriterion criterion, String element);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap that does not contain the given value -
     * key or value depending on the criterion parameter" (find by nested Map)
     *
     * @param criterion {@link AerospikeQueryCriterion#KEY or AerospikeQueryCriterion#VALUE}
     * @param element   Map key or value
     */
    List<P> findByFriendStringMapNotContaining(AerospikeQueryCriterion criterion, String element);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap that contains null value" (find by nested
     * Map)
     *
     * @param criterion     {@link AerospikeQueryCriterion#VALUE}
     * @param nullParameter {@link AerospikeNullQueryCriterion#NULL_PARAM}
     */
    List<P> findByFriendStringMapContaining(AerospikeQueryCriterion criterion,
                                            AerospikeNullQueryCriterion nullParameter);

    /**
     * Find all entities that satisfy the condition "have a friend with intMap that does not contain null value" (find
     * by nested Map)
     *
     * @param criterion     {@link AerospikeQueryCriterion#VALUE}
     * @param nullParameter {@link AerospikeNullQueryCriterion#NULL_PARAM}
     */
    List<P> findByFriendStringMapNotContaining(AerospikeQueryCriterion criterion,
                                               AerospikeNullQueryCriterion nullParameter);

    /**
     * Find all entities that satisfy the condition "does not have the given map key or does not have the given value"
     * (find by nested Map)
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key           Map key
     * @param value         Integer value to check
     */
    List<P> findByFriendIntMapNotContaining(AerospikeQueryCriterion criterionPair, String key, @NotNull Integer value);

    /**
     * Find all entities that satisfy the condition "have a friend with address (find by nested Map)"
     */
    List<P> findByFriendAddressExists();

    /**
     * Find all entities that satisfy the condition "have a friend with address which is null (i.e. friend's address
     * does not exist)" (find by nested Map)
     */
    List<P> findByFriendAddressIsNull();

    /**
     * Find all entities that satisfy the condition "have a friend with address which is not null (i.e. friend's address
     * exists)" (find by nested Map)
     */
    List<P> findByFriendAddressIsNotNull();

    /**
     * Find all entities that satisfy the condition "have a friend with address equal to the given argument" (find by
     * nested POJO)
     *
     * @param address - Address to check for equality
     */
    List<P> findByFriendAddress(Address address);

    /**
     * Find all entities that satisfy the condition "have a friend with address not equal to the given argument" (find
     * by nested Map)
     *
     * @param address - Address to check for equality
     */
    List<P> findByFriendAddressIsNot(Address address);

    /**
     * Find all entities that satisfy the condition "have a friend with address greater than or equal to the given
     * argument" (find by nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param address - Address to compare
     */
    List<P> findByFriendAddressGreaterThanEqual(Address address);

    /**
     * Find all entities that satisfy the condition "have a friend with address greater than the given argument" (find
     * by nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param address - Address to compare
     */
    List<P> findByFriendAddressGreaterThan(Address address);

    /**
     * Find all entities that satisfy the condition "have a friend with address less than or equal to the given
     * argument" (find by nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param address - Address to compare
     */
    List<P> findByFriendAddressLessThanEqual(Address address);

    /**
     * Find all entities that satisfy the condition "have a friend with address less than the given argument" (find by
     * nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param address - Address to compare
     */
    List<P> findByFriendAddressLessThan(Address address);

    /**
     * Find all entities that satisfy the condition "have a friend with address between the given arguments" (find by
     * nested Map)
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#map">Information about ordering</a>
     *
     * @param lowerLimit - lower limit, inclusive
     * @param upperLimit - upper limit, exclusive
     */
    List<P> findByFriendAddressBetween(Address lowerLimit, Address upperLimit);

    /**
     * Find all entities that satisfy the condition "have a friend with address equal to one of the values in the given
     * list" (find by nested Map)
     *
     * @param list - list of possible values
     */
    List<P> findByFriendAddressIn(List<Address> list);

    /**
     * Find all entities that satisfy the condition "have a friend with address equal to neither of the values in the
     * given list" (find by nested Map)
     *
     * @param list - list of possible values
     */
    List<P> findByFriendAddressNotIn(List<Address> list);

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
    List<P> findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddressZipCode(String zipCode);

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
    List<P> findByFriendFriendFriendFriendFriendFriendFriendFriendBestFriendAddress(Address address);

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
     * @param nullParameter {@link AerospikeNullQueryCriterion#NULL_PARAM}
     */
    List<P> findByStringsContaining(AerospikeNullQueryCriterion nullParameter);

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
     * @param nullParameter {@link AerospikeNullQueryCriterion#NULL_PARAM} to check for null
     */
    List<P> findByStringsNotContaining(AerospikeNullQueryCriterion nullParameter);

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
     * Find all entities that satisfy the condition "have ints list equal to one of the values in the given list" (find
     * by Collection)
     *
     * @param list - list of possible values, each of them subsequently gets converted to a List
     */
    List<P> findByIntsIn(List<Collection<Integer>> list);

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
     * Find all entities that satisfy the condition "have the list of lists which is greater than the given list".
     * <p>
     * ListOfIntLists is the name of the list of lists.
     * <br><br>
     * Note: only the upper level ListOfLists will be compared even if the parameter has different number of levels.
     * So findByListOfListsGreaterThan(List.of(1)) and findByListOfListsGreaterThan(List.of(List.of(List.of(1))))
     * will compare with the given parameter only the upper level ListOfLists itself
     *
     * </p>
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param list List to compare with
     */
    List<P> findByListOfIntListsGreaterThan(List<List<Integer>> list);

    /**
     * Find all entities that satisfy the condition "contain the given list".
     * <p>
     * @param list List to contain
     */
    List<P> findByListOfIntListsContaining(List<Integer> list);

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
     * Find all entities that satisfy the condition "have intSet in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    List<P> findByIntSetBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Find if there are entities that satisfy the condition "have intSet in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    boolean existsByIntSetBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Count entities that satisfy the condition "have intSet in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    long countByIntSetBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Find all entities that satisfy the condition "have intSet in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    void deleteByIntSetBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Find all entities that satisfy the condition "have ints list in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    List<P> findByIntsBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Find if there are entities that satisfy the condition "have ints list in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    boolean existsByIntsBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Count entities that satisfy the condition "have ints list in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    long countByIntsBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Delete entities that satisfy the condition "have ints list in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    void deleteByIntsBetween(Collection<Integer> from, Collection<Integer> to);

    /**
     * Find all entities that satisfy the condition "have strings list in the given range"
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#list">Information about ordering</a>
     *
     * @param from lower limit, inclusive, subsequently gets converted to a List
     * @param to   upper limit, exclusive, subsequently gets converted to a List
     */
    List<P> findByStringsBetween(Collection<String> from, Collection<String> to);

    P findFirstByLastNameStartingWith(String lastName, Sort sort);

    List<P> findTopByLastNameStartingWith(String lastName, Sort sort);

    List<P> findTop3ByLastNameStartingWith(String lastName, Sort sort);

    List<P> findFirst3ByLastNameStartingWith(String lastName, Sort sort);

    Page<P> findTop3ByLastNameStartingWith(String lastName, Pageable pageRequest);

    List<P> findByFirstName(String name);

    boolean existsByFirstName(String name);

    boolean existsByFirstNameIgnoreCase(String name);

    long countByFirstName(String name);

    /**
     * Delete all entities with the given first name
     *
     * @param name First name to match
     */
    void deleteByFirstName(String name);

    List<P> readByFirstName(String name);

    List<P> getByFirstName(String name);

    List<P> queryByFirstName(String name);

    List<P> searchByFirstName(String name);

    List<P> streamByFirstName(String name);

    List<P> findByGender(Person.Gender gender);

    List<P> findByGenderIn(List<Person.Gender> list);

    List<P> findByByteArray(byte[] byteArray);

    List<P> findByIntArray(int[] intArray);

    List<P> findByFirstNameIs(String name);

    boolean existsByFirstNameIs(String name);

    long countByFirstNameIs(String name);

    List<P> findByFirstNameEquals(String name);

    List<P> findByFirstNameNot(String name);

    List<P> findByFirstNameIsNot(String name);

    List<P> findByFirstNameExists();

    /**
     * Find all entities that satisfy the condition "have firstName higher in ordering than the given string".
     * <p>
     * <a href="https://docs.aerospike.com/server/guide/data-types/cdt-ordering#string">Information about ordering</a>
     *
     * @param string - String to compare with
     */
    List<P> findByFirstNameGreaterThan(String string);

    List<P> findByFirstNameAndAge(QueryParam string, QueryParam age);

    Iterable<P> findByAgeBetweenAndLastName(QueryParam ageBetween, QueryParam lastName);

    Iterable<P> findByAgeBetweenOrLastName(QueryParam ageBetween, QueryParam lastName);

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
