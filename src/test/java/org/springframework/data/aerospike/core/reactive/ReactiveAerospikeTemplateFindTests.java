package org.springframework.data.aerospike.core.reactive;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnRead;
import org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnReadAndExpirationProperty;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_MINUTE;

/**
 * Tests for find related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateFindTests extends BaseReactiveIntegrationTests {

    @Test
    public void findById_shouldReturnValueForExistingKey() {
        Person person = new Person(id, "Dave", "Matthews");
        StepVerifier.create(reactiveTemplate.save(person)).expectNext(person).verifyComplete();

        StepVerifier.create(reactiveTemplate.findById(id, Person.class)
            .subscribeOn(Schedulers.parallel())
        ).consumeNextWith(actual -> {
            assertThat(actual.getFirstName()).isEqualTo(person.getFirstName());
            assertThat(actual.getLastName()).isEqualTo(person.getLastName());
        }).verifyComplete();
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKey() {
        StepVerifier.create(reactiveTemplate.findById("dave-is-absent", Person.class)
                .subscribeOn(Schedulers.parallel())
            )
            .expectNextCount(0).verifyComplete();
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKeyIfTouchOnReadSetToTrue() {
        StepVerifier.create(reactiveTemplate.findById("foo-is-absent", DocumentWithTouchOnRead.class)
                .subscribeOn(Schedulers.parallel()))
            .expectNextCount(0).verifyComplete();
    }

    @Test
    public void findById_shouldIncreaseVersionIfTouchOnReadSetToTrue() {
        DocumentWithTouchOnRead document = new DocumentWithTouchOnRead(id, 1);
        StepVerifier.create(reactiveTemplate.save(document)).expectNext(document).verifyComplete();

        StepVerifier.create(reactiveTemplate.findById(document.getId(), DocumentWithTouchOnRead.class)
                .subscribeOn(Schedulers.parallel()))
            .consumeNextWith(actual -> assertThat(actual.getVersion()).isEqualTo(document.getVersion() + 1))
            .verifyComplete();
    }

    @Test
    public void findById_shouldFailOnTouchOnReadWithExpirationProperty() {
        DocumentWithTouchOnReadAndExpirationProperty document = new DocumentWithTouchOnReadAndExpirationProperty(id,
            EXPIRATION_ONE_MINUTE);
        reactiveTemplate.insert(document).block();

        assertThatThrownBy(() -> reactiveTemplate.findById(document.getId(),
                DocumentWithTouchOnReadAndExpirationProperty.class)
            .subscribeOn(Schedulers.parallel()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Touch on read is not supported for entity without expiration property");
    }

    @Test
    public void findByIds_shouldReturnEmptyList() {
        StepVerifier.create(reactiveTemplate.findByIds(Collections.emptyList(), Person.class)
                .subscribeOn(Schedulers.parallel()))
            .expectNextCount(0)
            .verifyComplete();
    }

    @Test
    public void findByIds_shouldFindExisting() {
        Person customer1 = new Person(nextId(), "Dave", "Matthews");
        Person customer2 = new Person(nextId(), "James", "Bond");
        Person customer3 = new Person(nextId(), "Matt", "Groening");
        reactiveTemplate.insertAll(Arrays.asList(customer1, customer2, customer3)).blockLast();

        List<String> ids = Arrays.asList("unknown", customer1.getId(), customer2.getId());
        List<Person> actual = reactiveTemplate.findByIds(ids, Person.class)
            .subscribeOn(Schedulers.parallel())
            .collectList().block();

        assertThat(actual).containsExactlyInAnyOrder(customer1, customer2);
    }
}
