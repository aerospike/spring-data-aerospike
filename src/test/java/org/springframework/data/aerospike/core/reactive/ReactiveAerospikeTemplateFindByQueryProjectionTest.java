package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.aerospike.util.QueryUtils;
import org.springframework.data.domain.Sort;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeTemplateFindByQueryProjectionTest extends BaseReactiveIntegrationTests {

    @BeforeAll
    public void beforeAllSetUp() {
        reactiveTemplate.deleteAll(Person.class).block();
        reactiveTemplate.deleteAll(OVERRIDE_SET_NAME).block();
    }

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        reactiveTemplate.deleteAll(Person.class).block();
        reactiveTemplate.deleteAll(OVERRIDE_SET_NAME).block();
    }

    @AfterAll
    public void afterAll() {
        reactiveTemplate.deleteAll(Person.class).block();
        reactiveTemplate.deleteAll(OVERRIDE_SET_NAME).block();
    }

    @Test
    public void findAll_findsAllExistingDocumentsProjection() {
        List<Person> persons = IntStream.rangeClosed(1, 10)
            .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(age).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons).blockLast();

        List<PersonSomeFields> result = reactiveTemplate.findAll(Person.class, PersonSomeFields.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSameElementsAs(persons.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()));
        deleteAll(persons); // cleanup
    }

    @Test
    public void findAllWithSetName_findsAllExistingDocumentsProjection() {
        List<Person> persons = IntStream.rangeClosed(1, 10)
            .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(age).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons, OVERRIDE_SET_NAME).blockLast();

        List<PersonSomeFields> result = reactiveTemplate.findAll(PersonSomeFields.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result)
            .hasSameElementsAs(persons.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()));
        deleteAll(persons, OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsProjection() {
        List<Person> allUsers = IntStream.range(20, 27)
            .mapToObj(id ->
                Person.builder().id(nextId()).firstName("Firstname").lastName("Lastname").build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<PersonSomeFields> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class,
                PersonSomeFields.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(actual)
            .hasSize(5)
            .containsAnyElementsOf(allUsers.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()));
        deleteAll(allUsers); // cleanup
    }

    @Test
    public void find_throwsExceptionForUnsortedQueryWithSpecifiedOffsetValueProjection() {
        Query query = new Query((Qualifier) null);
        query.setOffset(1);

        assertThatThrownBy(() -> reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    public void findByFilterEqualProjection() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
            .mapToObj(id ->
                Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findByFirstName", "Dave");

        List<PersonSomeFields> actual = reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(actual)
            .hasSize(10)
            .containsExactlyInAnyOrderElementsOf(allUsers.stream().map(Person::toPersonSomeFields)
                .collect(Collectors.toList()));
        deleteAll(allUsers); // cleanup
    }

    @Test
    public void findByFilterRangeProjection() {
        List<Person> allUsers = IntStream.rangeClosed(21, 30)
            .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave" + age).lastName("Matthews").age(age)
                .build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findCustomerByAgeBetween", 25, 31);

        List<PersonSomeFields> actual = reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(actual)
            .hasSize(6)
            .containsExactlyInAnyOrderElementsOf(
                allUsers.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()).subList(4, 10));
        deleteAll(allUsers); // cleanup
    }

    @Test
    public void findByFilterRangeProjectionWithSetName() {
        List<Person> allUsers = IntStream.rangeClosed(21, 30)
            .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave" + age).lastName("Matthews").age(age)
                .build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers, OVERRIDE_SET_NAME).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findCustomerByAgeBetween", 25, 31);

        List<PersonSomeFields> actual = reactiveTemplate.find(query, PersonSomeFields.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(actual)
            .hasSize(6)
            .containsExactlyInAnyOrderElementsOf(
                allUsers.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()).subList(4, 10));
        deleteAll(allUsers, OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void findByFilterRangeNonExistingProjection() {
        Query query = QueryUtils.createQueryForMethodWithArgs(serverVersionSupport, "findCustomerByAgeBetween", 100,
            150);

        List<PersonSomeFields> actual = reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(actual).isEmpty();
    }
}
