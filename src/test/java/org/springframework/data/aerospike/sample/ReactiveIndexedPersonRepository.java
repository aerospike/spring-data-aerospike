package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Reactive repository interface managing {@link IndexedPerson}s.
 */
public interface ReactiveIndexedPersonRepository extends ReactiveAerospikeRepository<IndexedPerson, String> {

    Flux<IndexedPerson> findByLastName(String lastName);

    Flux<IndexedPerson> findByAgeLessThan(int age);

    Flux<IndexedPerson> findByAgeBetween(int from, int to);

    /**
     * Find all entities that satisfy the condition "have address with zip code equal to the given argument" (find by
     * POJO field)
     *
     * @param zipCode - Zip code to check for equality
     */
    Flux<IndexedPerson> findByAddressZipCode(String zipCode);

    Flux<IndexedPerson> findPersonByFirstName(String firstName);

    Flux<IndexedPerson> findByAgeGreaterThan(int age);

    Mono<Page<IndexedPerson>> findByAgeGreaterThan(int value, Pageable pageable);

    Mono<Page<IndexedPerson>> findByAgeLessThan(int value, Pageable pageable);

    Flux<IndexedPerson> findByStringMapContaining(AerospikeQueryCriterion criteria, String element);

    /**
     * Find all entities that satisfy the condition "have exactly the given map key and value"
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key   Map key
     * @param value Value of the key
     */
    Flux<IndexedPerson> findByStringMapContaining(AerospikeQueryCriterion criterionPair, String key, String value);

    /**
     * Find all entities that satisfy the condition "have exactly the given map key and the given value"
     *
     * @param criterionPair {@link AerospikeQueryCriterion#KEY_VALUE_PAIR}
     * @param key   Map key
     * @param value Value of the key
     */
    Flux<IndexedPerson> findByIntMapContaining(AerospikeQueryCriterion criterionPair, String key, int value);

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

    Flux<IndexedPerson> findByFirstName(String string);

    Flux<IndexedPerson> findByLastNameStartingWith(String string);

    Flux<IndexedPerson> findDistinctByLastNameStartingWith(String string);

    /**
     * Distinct query for nested objects is currently not supported
     */
    Flux<IndexedPerson> findDistinctByFriendLastNameStartingWith(String string);

    Flux<IndexedPerson> findByFirstNameAndAge(QueryParam string, QueryParam i);

    Flux<IndexedPerson> findByAgeBetweenAndLastName(QueryParam ageBetween, QueryParam lastName);
}
