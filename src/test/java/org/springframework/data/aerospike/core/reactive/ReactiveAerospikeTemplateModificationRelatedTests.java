package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.HashMap;
import java.util.Map;


/**
 * Tests for save related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Yevhen Tsyba
 */
public class ReactiveAerospikeTemplateModificationRelatedTests extends BaseReactiveIntegrationTests {

    @Test
    public void shouldAdd() {
        // given
        Person one = Person.builder().id(id).age(25).build();
        Mono<Person> created = reactiveTemplate.insert(one);
        StepVerifier.create(created).expectNext(one).verifyComplete();

        // when
        Mono<Person> updated = reactiveTemplate.add(one, "age", 1).subscribeOn(Schedulers.parallel());

        // then
        StepVerifier.create(updated)
                .expectNext(Person.builder().id(id).age(26).build())
                .verifyComplete();
    }

    @Test
    public void shouldAppend() {
        // given
        Person one = Person.builder().id(id).firstName("Nas").build();
        Mono<Person> created = reactiveTemplate.insert(one).subscribeOn(Schedulers.parallel());
        StepVerifier.create(created).expectNext(one).verifyComplete();

        // when
        Mono<Person> appended = reactiveTemplate.append(one, "firstName", "tya").subscribeOn(Schedulers.parallel());

        // then
        Person expected = Person.builder().id(id).firstName("Nastya").build();
        StepVerifier.create(appended).expectNext(expected).verifyComplete();

        Mono<Person> storedPerson = reactiveTemplate.findById(id, Person.class);
        StepVerifier.create(storedPerson).expectNext(expected).verifyComplete();
    }

    @Test
    public void shouldAppendMultipleFields() {
        // given
        Person one = Person.builder().id(id).firstName("Nas").emailAddress("nastya@").build();
        Mono<Person> created = reactiveTemplate.insert(one).subscribeOn(Schedulers.parallel());
        StepVerifier.create(created).expectNext(one).verifyComplete();

        Map<String, String> toBeUpdated = new HashMap<>();
        toBeUpdated.put("firstName", "tya");
        toBeUpdated.put("email", "gmail.com");

        // when
        Mono<Person> appended = reactiveTemplate.append(one, toBeUpdated).subscribeOn(Schedulers.parallel());

        // then
        Person expected = Person.builder().id(id).firstName("Nastya").emailAddress("nastya@gmail.com").build();
        StepVerifier.create(appended).expectNext(expected).verifyComplete();

        Mono<Person> storedPerson = reactiveTemplate.findById(id, Person.class).subscribeOn(Schedulers.parallel());
        StepVerifier.create(storedPerson).expectNext(expected).verifyComplete();
    }

    @Test
    public void shouldPrepend() {
        // given
        Person one = Person.builder().id(id).firstName("tya").build();
        Mono<Person> created = reactiveTemplate.insert(one).subscribeOn(Schedulers.parallel());
        StepVerifier.create(created).expectNext(one).verifyComplete();

        // when
        Mono<Person> appended = reactiveTemplate.prepend(one, "firstName", "Nas")
                .subscribeOn(Schedulers.parallel());

        // then
        Person expected = Person.builder().id(id).firstName("Nastya").build();
        StepVerifier.create(appended).expectNext(expected).verifyComplete();

        Mono<Person> storedPerson = reactiveTemplate.findById(id, Person.class);
        StepVerifier.create(storedPerson).expectNext(expected).verifyComplete();
    }

    @Test
    public void shouldPrependMultipleFields() {
        // given
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        Mono<Person> created = reactiveTemplate.insert(one).subscribeOn(Schedulers.parallel());
        StepVerifier.create(created).expectNext(one).verifyComplete();

        Map<String, String> toBeUpdated = new HashMap<>();
        toBeUpdated.put("firstName", "Nas");
        toBeUpdated.put("email", "nastya@");

        // when
        Mono<Person> appended = reactiveTemplate.prepend(one, toBeUpdated).subscribeOn(Schedulers.parallel());

        // then
        Person expected = Person.builder().id(id).firstName("Nastya").emailAddress("nastya@gmail.com").build();
        StepVerifier.create(appended).expectNext(expected).verifyComplete();

        Mono<Person> storedPerson = reactiveTemplate.findById(id, Person.class).subscribeOn(Schedulers.parallel());
        StepVerifier.create(storedPerson).expectNext(expected).verifyComplete();
    }
}