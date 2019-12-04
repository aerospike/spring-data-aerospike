package org.springframework.data.aerospike.core.reactive;

import org.junit.Test;
import org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnRead;
import org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnReadAndExpirationProperty;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.Person;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_MINUTE;

/**
 * Tests for find related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateFindRelatedTests extends BaseReactiveAerospikeTemplateTests {
    @Test
    public void findById_shouldReturnValueForExistingKey() {
        Person person = new Person(id, "Dave", "Matthews");
        StepVerifier.create(reactiveTemplate.save(person)).expectNext(person).verifyComplete();

        StepVerifier.create(reactiveTemplate.findById(id, Person.class)).consumeNextWith(actual -> {
            assertThat(actual.getFirstName()).isEqualTo(person.getFirstName());
            assertThat(actual.getLastName()).isEqualTo(person.getLastName());
        }).verifyComplete();
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKey() {
        StepVerifier.create(reactiveTemplate.findById("dave-is-absent", Person.class))
                .expectNextCount(0).verifyComplete();
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKeyIfTouchOnReadSetToTrue() {
        StepVerifier.create(reactiveTemplate.findById("foo-is-absent", DocumentWithTouchOnRead.class))
                .expectNextCount(0).verifyComplete();

    }

    @Test
    public void findById_shouldIncreaseVersionIfTouchOnReadSetToTrue() {
        DocumentWithTouchOnRead document = new DocumentWithTouchOnRead(id, 1);
        StepVerifier.create(reactiveTemplate.save(document)).expectNext(document).verifyComplete();

        StepVerifier.create(reactiveTemplate.findById(document.getId(), DocumentWithTouchOnRead.class)).consumeNextWith(actual -> {
            assertThat(actual.getVersion()).isEqualTo(document.getVersion() + 1);
        }).verifyComplete();
    }

    @Test(expected = IllegalStateException.class)
    public void findById_shouldFailOnTouchOnReadWithExpirationProperty() {
        DocumentWithTouchOnReadAndExpirationProperty document = new DocumentWithTouchOnReadAndExpirationProperty(id, EXPIRATION_ONE_MINUTE);
        reactiveTemplate.insert(document).block();
        reactiveTemplate.findById(document.getId(), DocumentWithTouchOnReadAndExpirationProperty.class);
    }

    @Test
    public void findByIds_shouldReturnEmptyList() {
        Long userCount = reactiveTemplate.findByIds(Collections.emptyList(), Person.class).count().block();
        assertThat(userCount).isEqualTo(0);
    }

    @Test
    public void findByIds_shouldFindExisting() {
        Person customer1 = new Person(nextId(), "Dave", "Matthews");
        Person customer2 = new Person(nextId(), "James", "Bond");
        Person customer3 = new Person(nextId(), "Matt", "Groening");
        reactiveTemplate.insertAll(Arrays.asList(customer1, customer2, customer3)).blockLast();

        List<String> ids = Arrays.asList("unknown", customer1.getId(), customer2.getId());
        List<Person> actual = reactiveTemplate.findByIds(ids, Person.class).collectList().block();

        assertThat(actual).containsExactlyInAnyOrder(customer1, customer2);
    }

}