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

import org.springframework.data.aerospike.annotation.Query;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * This repository acts as a storage for invalid method names used for testing. For actual repository see
 * {@link PersonRepository}
 */
public interface PersonNegativeTestsRepository<P extends Person> extends AerospikeRepository<P, String> {

    /**
     * Type mismatch: expecting String
     */
    List<P> findByFirstName(int number);

    /**
     * Type mismatch: expecting Integer
     */
    List<P> findByAgeNot(String string);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByLastName(String name1, String name2);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByAgeNot(int age1, int age2);

    /**
     * Invalid number of arguments: at least two arguments are required
     */
    List<P> findByStringMapContaining(String key);

    /**
     * Invalid number of arguments: at least two arguments are required
     */
    List<P> findByStringMapNotContaining(String key);

    /**
     * Invalid number of arguments: at least two arguments are required
     */
    List<P> findByStringMapContaining(int value);

    /**
     * Invalid number of arguments: at least two arguments are required
     */
    List<P> findByStringMapNotContaining(int value);

    /**
     * Invalid number of arguments: at least two arguments are required
     */
    List<P> findByStringMapContaining();

    /**
     * Invalid number of arguments: at least two arguments are required
     */
    List<P> findByStringMapNotContaining();

    /**
     * Invalid map key type at position 2
     */
    List<P> findByStringMapContaining(CriteriaDefinition.AerospikeQueryCriterion criterion1,
                                      CriteriaDefinition.AerospikeQueryCriterion criterion2);

    /**
     * Invalid map key type at position 2
     */
    List<P> findByStringMapNotContaining(CriteriaDefinition.AerospikeQueryCriterion criterion1,
                                         CriteriaDefinition.AerospikeQueryCriterion criterion2);

    /**
     * Invalid first argument type, required AerospikeQueryCriterion
     */
    List<P> findByStringMapContaining(String key1, String value1, Person key2, String value2);

    /**
     * Invalid first argument type: required AerospikeQueryCriterion
     */
    List<P> findByStringMapNotContaining(String key1, String value1, Person key2, String value2);

    /**
     * Invalid combination of arguments: expecting Map
     */
    List<P> findByStringMapEquals(String obj);

    /**
     * Invalid combination of arguments: expecting Map
     */
    List<P> findByStringMapIsNot(String obj);

    /**
     * Invalid combination of arguments: expecting Map
     */
    List<P> findByStringMap(int obj);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringMapEquals(Map<String, String> map1, Map<String, String> map2);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringMapIsNot(Map<String, String> map1, Map<String, String> map2);

    /**
     * Invalid map key type at position 2
     */
    List<P> findByIntMapContaining(CriteriaDefinition.AerospikeQueryCriterion criterion,
                                   int number);

    /**
     * Invalid map key/value type at position 2
     */
    List<P> findByIntMapContaining(CriteriaDefinition.AerospikeQueryCriterion criterion,
                                   String string);

    /**
     * Invalid map value type at position 3
     */
    List<P> findByIntMapContaining(CriteriaDefinition.AerospikeQueryCriterion criterion,
                                   String key, String value);

    /**
     * Invalid argument type: expecting Map
     */
    List<P> findByIntMapLessThan(int number1);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByIntMapLessThan(int number1, int number2, int number3);

    /**
     * Invalid argument type: expecting Map
     */
    List<P> findByIntMapLessThan(Person obj, int number);

    /**
     * Invalid number of arguments: expecting two
     */
    List<P> findByIntMapBetween();

    /**
     * Invalid number of arguments: expecting two
     */
    List<P> findByIntMapBetween(int number1);

    /**
     * Invalid argument type: expecting Map
     */
    List<P> findByIntMapBetween(int number1, int number2);

    /**
     * Invalid argument type: expecting Map
     */
    List<P> findByIntMapBetween(int number1, Map<Integer, Integer> map);

    /**
     * Invalid number of arguments: expecting two
     */
    List<P> findByIntMapBetween(int number1, int number2, int number3, int number4);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringsIsNot();

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStrings();

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringsEquals(String string1, String string2);

    /**
     * Invalid number of arguments: expecting one
     */
    @Query(expression = "$.strings == ?0")
    List<P> findByStringsEquals(Collection<String> strings);
    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStrings(Collection<String> collection1, Collection<String> collection2);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringsIsNot(String string1, String string2);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringsIsNot(List<?> list1, List<?> list2);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringsLessThan();

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringsLessThan(List<?> list1, List<?> list2);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByStringsLessThan(String string1, String string2);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByIntsGreaterThan(int number1, int number2);

    /**
     * Invalid argument type: expecting Collection
     */
    List<P> findByIntsGreaterThan(int number);

    /**
     * Invalid number of arguments: expecting at least one
     */
    List<P> findByIntsContaining();

    /**
     * Invalid number of arguments: expecting at least one
     */
    List<P> findByIntsNotContaining();

    /**
     * Invalid number of arguments: expecting two
     */
    List<P> findByIntsBetween();

    /**
     * Invalid number of arguments: expecting two
     */
    boolean existsByIntsBetween();

    /**
     * Invalid number of arguments: expecting two
     */
    long countByIntsBetween();

    /**
     * Invalid number of arguments: expecting two
     */
    void deleteByIntsBetween();

    /**
     * Invalid number of arguments: expecting two
     */
    List<P> findByIntsBetween(int number1);

    /**
     * Invalid number of arguments: expecting two
     */
    boolean existsByIntsBetween(int number1);

    /**
     * Invalid number of arguments: expecting two
     */
    long countByIntsBetween(int number1);

    /**
     * Invalid number of arguments: expecting two
     */
    void deleteByIntsBetween(int number1);

    /**
     * Type mismatch, expecting one of the following types: Number, Collection
     */
    List<P> findByIntsBetween(Map<Integer, Integer> map1, Map<Integer, Integer> map2);

    /**
     * Type mismatch, expecting one of the following types: Number, Collection
     */
    boolean existsByIntsBetween(Map<Integer, Integer> map1, Map<Integer, Integer> map2);

    /**
     * Type mismatch, expecting one of the following types: Number, Collection
     */
    long countByIntsBetween(Map<Integer, Integer> map1, Map<Integer, Integer> map2);

    /**
     * Type mismatch, expecting one of the following types: Number, Collection
     */
    long existsBycountByIntsBetween(Map<Integer, Integer> map1, Map<Integer, Integer> map2);

    /**
     * Type mismatch, expecting one of the following types: Number, Collection
     */
    void deleteByIntsBetween(Map<Integer, Integer> map1, Map<Integer, Integer> map2);

    /**
     * Invalid number of arguments: expecting one POJO
     */
    List<P> findByAddress();

    /**
     * Invalid number of arguments: expecting one POJO
     */
    List<P> findByAddressEquals();

    /**
     * Invalid number of arguments: expecting one POJO
     */
    List<P> findByAddress(Address address1, Address address2);

    /**
     * Invalid number of arguments: expecting one POJO
     */
    List<P> findByAddressIsNot();

    /**
     * Type mismatch: expecting Address
     */
    List<P> findByAddress(int number1);

    /**
     * Type mismatch: expecting Address
     */
    List<P> findByAddressIsNot(int number1);

    /**
     * Invalid number of arguments: expecting one POJO
     */
    List<P> findByFriendAddress();

    /**
     * Invalid number of arguments: expecting one POJO
     */
    List<P> findByFriendAddressEquals();

    /**
     * Invalid number of arguments: expecting one POJO
     */
    List<P> findByFriendAddressIsNot();

    /**
     * Type mismatch: expecting Address
     */
    List<P> findByFriendAddress(int number1);

    /**
     * Type mismatch: expecting Address
     */
    List<P> findByFriendAddressIsNot(int number1);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByFriendAddressZipCode();

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByFriendAddressZipCodeEquals();

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByFriendAddressZipCodeIsNot();

    /**
     * Invalid number of arguments: expecting two POJOs
     */
    List<P> findByAddressBetween();

    /**
     * Invalid number of arguments: expecting two POJOs
     */
    List<P> findByAddressBetween(int number1);

    /**
     * Type mismatch, expecting Address
     */
    List<P> findByAddressBetween(int number1, int number2);

    /**
     * Invalid number of arguments: expecting two POJOs
     */
    List<P> findByAddressBetween(int number1, int number2, int number3);

    /**
     * Expected CombinedQueryParam, instead got String
     */
    List<P> findByFirstNameAndAge(String firstName, int age);

    /**
     * Invalid number of arguments, expecting one
     */
    List<P> findByFirstNameOrAge(QueryParam firstName);

    /**
     * Expecting no arguments
     */
    List<P> findByAddressExists(Address address);

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByIsActive();

    /**
     * Invalid number of arguments: expecting one
     */
    List<P> findByIsActive(boolean value1, boolean value2);
}
