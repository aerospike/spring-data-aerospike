package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.domain.Sort;
import reactor.core.scheduler.Schedulers;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.domain.Sort.Order.asc;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeTemplateFindInRangeTests extends ReactiveAerospikeTemplateFindByQueryTests {

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

        deleteAll(allUsers); // cleanup
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

        deleteAll(allUsers); // cleanup
    }

    @Test
    public void findInRangeWithSetName_shouldFindLimitedNumberOfDocumentsAndSkip() {
        List<Person> allUsers = IntStream.range(20, 27)
            .mapToObj(id -> new Person(nextId(), "Firstname", "Lastname")).collect(Collectors.toList());
        reactiveTemplate.insertAll(allUsers, OVERRIDE_SET_NAME).blockLast();

        List<Person> actual = reactiveTemplate.findInRange(0, 5, Sort.unsorted(), Person.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(actual)
            .hasSize(5)
            .containsAnyElementsOf(allUsers);

        deleteAll(allUsers, OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void findInRange_shouldFindLimitedNumberOfDocumentsWithOrderBy() {
        List<Person> persons = new ArrayList<>();
        persons.add(new Person(nextId(), "Dave", "Matthews"));
        persons.add(new Person(nextId(), "Josh", "Matthews"));
        persons.add(new Person(nextId(), "Chris", "Yes"));
        persons.add(new Person(nextId(), "Kate", "New"));
        persons.add(new Person(nextId(), "Nicole", "Joshua"));
        reactiveTemplate.insertAll(persons).blockLast();

        int skip = 0;
        int limit = 3;
        Sort sort = Sort.by(asc("firstName"));

        List<Person> result = reactiveTemplate.findInRange(skip, limit, sort, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(Objects.requireNonNull(result).stream().map(Person::getFirstName).collect(Collectors.toList()))
            .hasSize(3)
            .containsExactly("Chris", "Dave", "Josh");

        deleteAll(persons); // cleanup
    }
}
