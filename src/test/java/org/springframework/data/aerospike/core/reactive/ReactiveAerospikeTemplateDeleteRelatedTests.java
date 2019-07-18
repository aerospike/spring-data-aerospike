package org.springframework.data.aerospike.core.reactive;

import org.junit.Before;
import org.junit.Test;
import org.springframework.data.aerospike.core.Person;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

/**
 * Tests for delete related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Yevhen Tsyba
 */
public class ReactiveAerospikeTemplateDeleteRelatedTests extends BaseReactiveAerospikeTemplateTests {
    private String id;

    @Before
    public void setUp() {
        this.id = nextId();
    }

    @Test
    public void testSimpleDeleteById() {
        // given
        Person person = new Person(id, "QLastName", 21);

        Mono<Person> created = reactiveTemplate.insert(person);
        StepVerifier.create(created).expectNext(person).verifyComplete();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(id, Person.class);
        StepVerifier.create(deleted).expectNext(true).verifyComplete();

        // then
        Mono<Person> result = reactiveTemplate.findById(id, Person.class);
        StepVerifier.create(result).expectComplete().verify();;
    }

    @Test
    public void testSimpleDeleteByObject() {
        // given
        Person person = new Person(id, "QLastName", 21);

        Mono<Person> created = reactiveTemplate.insert(person);
        StepVerifier.create(created).expectNext(person).verifyComplete();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(person);
        StepVerifier.create(deleted).expectNext(true).verifyComplete();

        // then
        Mono<Person> result = reactiveTemplate.findById(id, Person.class);
        StepVerifier.create(result).expectComplete().verify();
    }

    @Test
    public void deleteById_shouldReturnFalseIfValueIsAbsent() {
        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(id, Person.class);

        // then
        StepVerifier.create(deleted).expectComplete().verify();
    }

    @Test
    public void deleteByObject_shouldReturnFalseIfValueIsAbsent() {
        // given
        Person person = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();

        // when
        Mono<Boolean> deleted = reactiveTemplate.delete(person);

        // then
        StepVerifier.create(deleted).expectComplete().verify();
    }
}
