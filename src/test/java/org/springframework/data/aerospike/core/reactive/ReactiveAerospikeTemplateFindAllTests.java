package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeTemplateFindAllTests extends ReactiveAerospikeTemplateFindByQueryTests {

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

        deleteAll(persons); // cleanup
    }

    @Test
    public void findAllWithSetName_findAllExistingDocuments() {
        List<Person> persons = IntStream.rangeClosed(1, 10)
            .mapToObj(age -> Person.builder().id(nextId()).firstName("Dave").lastName("Matthews").age(age).build())
            .collect(Collectors.toList());
        reactiveTemplate.insertAll(persons, OVERRIDE_SET_NAME).blockLast();

        List<Person> result = reactiveTemplate.findAll(Person.class, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();
        assertThat(result).hasSameElementsAs(persons);

        deleteAll(persons, OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void findAll_findNothing() {
        List<Person> actual = reactiveTemplate.findAll(Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(actual).isEmpty();
    }
}
