package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.QueryUtils;
import org.springframework.data.aerospike.repository.query.Query;
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
        blockingAerospikeTestOperations.deleteAll(Person.class);

        blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "age_index", "age", IndexType.NUMERIC);
        blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "last_name_index", "lastName", IndexType.STRING);
        blockingAerospikeTestOperations.createIndexIfNotExists(Person.class, "first_name_index", "firstName", IndexType.STRING);
    }

    @Test
    public void findAll_findsAllExistingDocuments() {
        List<Person> persons = IntStream.rangeClosed(1, 10)
                .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(age).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        List<Person> result = reactiveTemplate.findAll(Person.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(result).containsOnlyElementsOf(persons);
    }

    @Test
    public void findAll_findsNothing() {
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

}
