package org.springframework.data.aerospike.core.reactive.find;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.annotation.Nightly;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

@Nightly
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

    @Test
    public void findAll_findIdOnlyRecord() {
        var id = 100;
        var doc = new SampleClasses.DocumentWithPrimitiveIntId(id); // id-only document
        var clazz = SampleClasses.DocumentWithPrimitiveIntId.class;

        var existingDoc = reactiveTemplate.findById(id, clazz).block();
        assertThat(existingDoc).withFailMessage("The same record already exists").isNull();

        reactiveTemplate.insert(doc).block();
        var resultsFindById = reactiveTemplate.findById(id, clazz).block();
        assertThat(resultsFindById).withFailMessage("findById error").isEqualTo(doc);
        var resultsFindAll = reactiveTemplate.findAll(clazz).collectList().block();
        // findAll() must correctly find the record that contains id and no bins
        assertThat(resultsFindAll).size().withFailMessage("findAll error").isEqualTo(1);

        // cleanup
        reactiveTemplate.delete(doc);
    }
}
