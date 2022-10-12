package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.QueryUtils;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.PersonSomeFields;
import org.springframework.data.domain.Sort;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ReactiveAerospikeTemplateFindByQueryProjectionTest extends BaseReactiveIntegrationTests {

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        additionalAerospikeTestOperations.deleteAll(Person.class);

        additionalAerospikeTestOperations.createIndexIfNotExists(
                Person.class, "person_age_index", "age", IndexType.NUMERIC);
        additionalAerospikeTestOperations.createIndexIfNotExists(
                Person.class, "person_last_name_index", "lastName", IndexType.STRING);
        additionalAerospikeTestOperations.createIndexIfNotExists(
                Person.class, "person_first_name_index", "firstName", IndexType.STRING);
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
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsProjection() {
        List<Person> allUsers = IntStream.range(20, 27)
                .mapToObj(id ->
                        new Person(nextId(), "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        List<PersonSomeFields> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class,
                        PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(actual)
                .hasSize(5)
                .containsAnyElementsOf(allUsers.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()));
    }

    @Test
    public void find_throwsExceptionForUnsortedQueryWithSpecifiedOffsetValueProjection() {
        Query query = new Query((Sort) null);
        query.setOffset(1);

        assertThatThrownBy(() -> reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Unsorted query must not have offset value. For retrieving paged results use sorted query.");
    }

    @Test
    public void find_shouldWorkWithFilterEqualProjection() {
        List<Person> allUsers = IntStream.rangeClosed(1, 10)
                .mapToObj(id -> new Person(nextId(), "Dave", "Matthews")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findPersonByFirstName", "Dave");

        List<PersonSomeFields> actual = reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();
        assertThat(actual)
                .hasSize(10)
                .containsExactlyInAnyOrderElementsOf(allUsers.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()));
    }

    @Test
    public void find_shouldWorkWithFilterRangeProjection() {
        List<Person> allUsers = IntStream.rangeClosed(21, 30)
                .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave" + age).lastName("Matthews").age(age).build())
                .collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers).blockLast();

        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 25, 30);

        List<PersonSomeFields> actual = reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();

        assertThat(actual)
                .hasSize(6)
                .containsExactlyInAnyOrderElementsOf(
                        allUsers.stream().map(Person::toPersonSomeFields).collect(Collectors.toList()).subList(4, 10));
    }

    @Test
    public void find_shouldWorkWithFilterRangeNonExistingProjection() {
        Query query = QueryUtils.createQueryForMethodWithArgs("findCustomerByAgeBetween", 100, 150);

        List<PersonSomeFields> actual = reactiveTemplate.find(query, Person.class, PersonSomeFields.class)
                .subscribeOn(Schedulers.parallel())
                .collectList().block();

        assertThat(actual).isEmpty();
    }
}
