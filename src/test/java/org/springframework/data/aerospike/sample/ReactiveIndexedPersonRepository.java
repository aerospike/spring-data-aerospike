package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import reactor.core.publisher.Flux;

/**
 * Reactive repository interface managing {@link IndexedPerson}s.
 */
public interface ReactiveIndexedPersonRepository extends ReactiveAerospikeRepository<IndexedPerson, String> {

    Flux<IndexedPerson> findByLastName(String lastName);

    Flux<IndexedPerson> findByAgeLessThan(int age);

    Flux<IndexedPerson> findByAgeBetween(int from, int to);

    Flux<IndexedPerson> findByAddress(Address address);

    Flux<IndexedPerson> findByAddressZipCode(String zipCode);

    Flux<IndexedPerson> findPersonByFirstName(String firstName);

    Flux<IndexedPerson> findByAgeGreaterThan(int age);

    Flux<IndexedPerson> findByStringMapContaining(String element, CriteriaDefinition.AerospikeMapCriteria criteria);

    /**
     * Find all entities that satisfy the condition "have exactly the given map key and the given value"
     *
     * @param key   Map key
     * @param value Value of the key
     */
    Flux<IndexedPerson> findByStringMapEquals(String key, String value);

    /**
     * Find all entities that satisfy the condition "have exactly the given map key and the given value"
     *
     * @param key   Map key
     * @param value Value of the key
     */
    Flux<IndexedPerson> findByIntMapEquals(String key, int value);

    /**
     * Find all entities that satisfy the condition "have the given map key and a value that is greater than the given
     * integer"
     *
     * @param key         Map key
     * @param greaterThan integer to check if value is greater than it
     */
    Flux<IndexedPerson> findByIntMapGreaterThan(String key, int greaterThan);

    /**
     * Find all entities that satisfy the condition "have the given map key and a value that is less than or equal to
     * the given integer"
     *
     * @param key               Map key
     * @param lessThanOrEqualTo integer to check if value satisfies the condition
     */
    Flux<IndexedPerson> findByIntMapLessThanEqual(String key, int lessThanOrEqualTo);

    /**
     * Find all entities that satisfy the condition "have the given map key and a value in between the given integers"
     *
     * @param key  Map key
     * @param from the lower limit for the map value, inclusive
     * @param to   the upper limit for the map value, inclusive
     */
    Flux<IndexedPerson> findByIntMapBetween(String key, int from, int to);

    Flux<IndexedPerson> findByFriendLastName(String value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age equal to the given integer" (find by
     * POJO field)
     *
     * @param value - number to check for equality
     */
    Flux<IndexedPerson> findByFriendAge(int value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age greater than the given integer" (find by
     * POJO field)
     *
     * @param value - lower limit, exclusive
     */
    Flux<IndexedPerson> findByFriendAgeGreaterThan(int value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age less than or equal to the given integer"
     * (find by POJO field)
     *
     * @param value - upper limit, inclusive
     */
    Flux<IndexedPerson> findByFriendAgeLessThanEqual(int value);

    /**
     * Find all entities that satisfy the condition "have a friend with the age in the given range" (find by POJO
     * field)
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, inclusive
     */
    Flux<IndexedPerson> findByFriendAgeBetween(int from, int to);

    /**
     * Find all entities that satisfy the condition "have the list which contains the given string"
     * <p>
     * List name in this case is Strings
     * </p>
     *
     * @param string string to check
     */
    Flux<IndexedPerson> findByStringsContaining(String string);

    /**
     * Find all entities that satisfy the condition "have the list which contains the given integer"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer number to check
     */
    Flux<IndexedPerson> findByIntsContaining(int integer);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is greater than the given
     * integer"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer upper limit, exclusive
     */
    Flux<IndexedPerson> findByIntsGreaterThan(int integer);

    /**
     * Find all entities that satisfy the condition "have at least one list value which is less than or equal to the
     * given integer"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param integer upper limit, inclusive
     */
    Flux<IndexedPerson> findByIntsLessThanEqual(int integer);

    /**
     * Find all entities that satisfy the condition "have at least one list value in the given range"
     * <p>
     * List name in this case is Ints
     * </p>
     *
     * @param from lower limit, inclusive
     * @param to   upper limit, inclusive
     */
    Flux<IndexedPerson> findByIntsBetween(int from, int to);

    Flux<IndexedPerson> findByFirstName(String string);

    Flux<IndexedPerson> findByFirstNameAndAge(String string, int i);

    Flux<IndexedPerson> findByAgeBetweenAndLastName(int from, int to, String lastName);
}
