package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.QueryUtils;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Address;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Sort;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReactiveAerospikeTemplateFindByQueryTests extends BaseReactiveIntegrationTests {

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        additionalAerospikeTestOperations.deleteAll(Person.class);

        additionalAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_age_index", "age", IndexType.NUMERIC);
        additionalAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_last_name_index", "lastName", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(Person.class, "person_first_name_index", "firstName", IndexType.STRING);
    }

    @Test
    public void findAll_findAllExistingDocuments() {
        List<Person> persons = IntStream.rangeClosed(1, 10)
                .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(age).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        List<Person> result = reactiveTemplate.findAll(Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result).hasSameElementsAs(persons);
    }

    @Test
    public void findAll_findNothing() {
        StepVerifier.create(reactiveTemplate.findAll(Person.class)
                .subscribeOn(Schedulers.parallel()))
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocuments() {
        List<Person> allUsers = IntStream.range(20, 27)
                .mapToObj(id -> new Person(nextId(), "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<Person> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(actual)
                .hasSize(5)
                .containsAnyElementsOf(allUsers);
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsAndSkip() {
        List<Person> allUsers = IntStream.range(20, 27)
                .mapToObj(id -> new Person(nextId(), "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<Person> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();

        assertThat(actual)
                .hasSize(5)
                .containsAnyElementsOf(allUsers);
    }

    @Test
    public void find_throwsExceptionForUnsortedQueryWithSpecifiedOffsetValue() {
        Query query = new Query((Sort) null);
        query.setOffset(1);

        assertThatThrownBy(() -> reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    public void find_shouldWorkWithFilterEqual() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person(nextId(), "Dave", "Matthews")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findPersonByFirstName", "Dave");

        List<Person> actual = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyInAnyOrderElementsOf(allUsers);
    }

    @Test
    public void find_shouldWorkWithFilterEqualOrderBy() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person(nextId(), "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort(Comparator.comparing(Person::getFirstName)); // Order user list by firstname ascending

        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameAsc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyElementsOf(allUsers);
    }

    @Test
    public void find_shouldWorkWithFilterEqualOrderByDesc() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person(nextId(), "Dave" + id, "Matthews")).collect(Collectors.toList());
        Collections.shuffle(allUsers); // Shuffle user list
        reactiveTemplate.insertAll(allUsers).blockLast();
        allUsers.sort((o1, o2) -> o2.getFirstName().compareTo(o1.getFirstName())); // Order user list by firstname descending

        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameDesc", "Matthews");

        List<Person> actual = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyElementsOf(allUsers);
    }

    @Test
    public void find_shouldWorkWithFilterRange() {
        List<Person> allUsers = IntStream.rangeClosed(21, 30)
                .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave" + age).lastName("Matthews").age(age).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 25, 30);

        List<Person> actual = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();

        assertThat(actual)
                .hasSize(6)
                .containsExactlyInAnyOrderElementsOf(allUsers.subList(4, 10));
    }

    @Test
    public void find_shouldWorkWithFilterRangeNonExisting() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 100, 150);

        List<Person> actual = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();

        assertThat(actual).isEmpty();
    }

    @Test
    public void findWithFilterEqualOrderByDescNonExisting() {
        Object[] args = {"NonExistingSurname"};
        Query query = QueryUtils.createQueryForMethodWithArgs("findByLastNameOrderByFirstNameDesc", args);

        Flux<Person> result = reactiveTemplate.find(query, Person.class);

        StepVerifier.create(result)
                .expectNextCount(0)
                .verifyComplete();
    }

    @Test
    public void findByListContainingInteger() {
        List<Person> persons = IntStream.rangeClosed(1, 7)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").ints(Collections.singletonList(100 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsContaining", 100);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));
    }

    @Test
    public void findByListContainingString() {
        List<Person> persons = IntStream.rangeClosed(1, 7)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").strings(Collections.singletonList("str" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringsContaining", "str2");

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(1, 2));
    }

    @Test
    public void findByListValueLessThanOrEqual() {
        List<Person> persons = IntStream.rangeClosed(1, 7)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").ints(Collections.singletonList(100 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsLessThanEqual", 500);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(5)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 5));
    }

    @Test
    public void findByListValueInRange() {
        List<Person> persons = IntStream.rangeClosed(1, 7)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").ints(Collections.singletonList(100 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsBetween", 200, 600);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(5)
                .containsExactlyInAnyOrderElementsOf(persons.subList(1, 6));
    }

    @Test
    public void findByListValueGreaterThan() {
        List<Person> persons = IntStream.rangeClosed(1, 7)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").ints(Collections.singletonList(100 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntsGreaterThan", 549);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(2)
                .containsExactlyInAnyOrderElementsOf(persons.subList(5, 7));
    }

    @Test
    public void findByMapKeysContaining() {
        List<Person> persons = IntStream.rangeClosed(1, 2)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapContaining", "key1", CriteriaDefinition.AerospikeMapCriteria.KEY);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));
    }

    @Test
    public void findByMapValuesContaining() {
        List<Person> persons = IntStream.rangeClosed(1, 2)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapContaining", "val1", CriteriaDefinition.AerospikeMapCriteria.VALUE);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));
    }

    @Test
    public void findByMapKeyValueEquals() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapEquals", "key1", "val1");

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));
    }

    @Test
    public void findByMapKeyValueNotEquals() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").intMap(Collections.singletonMap("key" + id, id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntMapIsNot", "key1", 11);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 1));
    }

    @Test
    public void findByMapKeyValueGreaterThan() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").intMap(Collections.singletonMap("key" + id, id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntMapGreaterThan", "key2", 1);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(1, 2));
    }

    @Test
    public void findByMapKeyValueBetween() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").intMap(Collections.singletonMap("key" + id, id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByIntMapBetween", "key3", 0, 4);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(2, 3));
    }

    @Test
    public void findByMapKeyValueStartsWith() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapStartsWith", "key4", "val");

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(3, 4));
    }

    @Test
    public void findByMapKeyValueContains() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").stringMap(Collections.singletonMap("key" + id, "val" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByStringMapContaining", "key3", "al");

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(2, 3));
    }

    @Test
    public void findPersonsByFriendAge() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAge", 50);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(4, 5));
    }

    @Test
    public void findPersonsByFriendAgeNotEqual() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeIsNot", 50);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(4)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 4));
    }

    @Test
    public void findPersonsByFriendAgeGreaterThan() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeGreaterThan", 42);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(4, 5));
    }

    @Test
    public void findPersonsByFriendAgeLessThanOrEqual() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeLessThanEqual", 42);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(4)
                .containsExactlyInAnyOrderElementsOf(persons.subList(0, 4));
    }

    @Test
    public void findPersonsByFriendAgeRange() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").friend(new Person("person" + id, "Leroi" + id, 10 * id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByFriendAgeBetween", 42, 50);

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(4, 5));
    }

    @Test
    public void findPersonsByAddressZipCode() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").address(new Address("Foo Street " + id, "C012" + id, "City" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByAddressZipCode", "C0123");

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(2, 3));
    }

    @Test
    public void findByAddressZipCodeContaining() {
        List<Person> persons = IntStream.rangeClosed(1, 5)
                .mapToObj(id -> Person.builder().id(nextId()).firstName("Dave" + id).lastName("Matthews").address(new Address("Foo Street " + id, "C012" + id, "City" + id)).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findByAddressZipCodeContaining", "123");

        List<Person> result = reactiveTemplate.find(query, Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result)
                .hasSize(1)
                .containsExactlyInAnyOrderElementsOf(persons.subList(2, 3));
    }
}
