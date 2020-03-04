package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import org.junit.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.SampleClasses.DocumentWithByteArray;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

public class ReactiveAerospikeTemplateInsertTests extends BaseReactiveIntegrationTests {

    @Test
    public void insertsAndFindsWithCustomCollectionSet() {
        CustomCollectionClass initial = new CustomCollectionClass(id, "data0");
        reactiveTemplate.insert(initial).block();

        StepVerifier.create(reactorClient.get(new Policy(), new Key(getNameSpace(), "custom-set", id)))
                .assertNext(keyRecord -> assertThat(keyRecord.record.getString("data")).isEqualTo("data0"))
                .verifyComplete();
        assertThat(findById(id, CustomCollectionClass.class)).isEqualTo(initial);
    }

    @Test
    public void insertsDocumentWithListMapDateStringLongValues() {
        Person customer = Person.builder()
                .id(id)
                .firstName("Dave")
                .lastName("Grohl")
                .age(45)
                .waist(90)
                .emailAddress("dave@gmail.com")
                .map(Collections.singletonMap("k", "v"))
                .list(Arrays.asList("a", "b", "c"))
                .friend(new Person(null, "Anna", 43))
                .active(true)
                .sex(Person.Sex.MALE)
                .dateOfBirth(new Date())
                .build();

        StepVerifier.create(reactiveTemplate.insert(customer))
                .expectNext(customer)
                .verifyComplete();

        Person actual = findById(id, Person.class);
        assertThat(actual).isEqualTo(customer);
    }

    @Test
    public void insertsAndFindsDocumentWithByteArrayField() {
        DocumentWithByteArray document = new DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});

        reactiveTemplate.insert(document).block();

        DocumentWithByteArray result = findById(id, DocumentWithByteArray.class);
        assertThat(result).isEqualTo(document);
    }

    @Test
    public void insertsDocumentWithNullFields() {
        VersionedClass document = new VersionedClass(id, null);

        reactiveTemplate.insert(document).block();

        assertThat(document.getField()).isNull();
    }

    @Test
    public void insertsDocumentWithZeroVersionIfThereIsNoDocumentWithSameKey() {
        VersionedClass document = new VersionedClass(id, "any");

        reactiveTemplate.insert(document).block();

        assertThat(document.getVersion()).isEqualTo(1);
    }

    @Test
    public void insertsDocumentWithVersionGreaterThanZeroIfThereIsNoDocumentWithSameKey() {
        VersionedClass document = new VersionedClass(id, "any", 5L);

        reactiveTemplate.insert(document).block();

        assertThat(document.getVersion()).isEqualTo(1);
    }

    @Test
    public void throwsExceptionForDuplicateId() {
        Person person = new Person(id, "Amol", 28);

        reactiveTemplate.insert(person).block();
        StepVerifier.create(reactiveTemplate.insert(person))
                .expectError(DuplicateKeyException.class)
                .verify();
    }

    @Test
    public void throwsExceptionForDuplicateIdForVersionedDocument() {
        VersionedClass document = new VersionedClass(id, "any", 5L);

        reactiveTemplate.insert(document).block();
        StepVerifier.create(reactiveTemplate.insert(document))
                .expectError(DuplicateKeyException.class)
                .verify();
    }

    @Test
    public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForVersionedDocument() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicLong duplicateKeyCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            reactiveTemplate.insert(new VersionedClass(id, data))
                    .onErrorResume(DuplicateKeyException.class, e -> {
                        duplicateKeyCounter.incrementAndGet();
                        return Mono.empty();
                    })
                    .block();
        });

        assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
    }

    @Test
    public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForNonVersionedDocument() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicLong duplicateKeyCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            reactiveTemplate.insert(new Person(id, data, 28))
                    .onErrorResume(DuplicateKeyException.class, e -> {
                        duplicateKeyCounter.incrementAndGet();
                        return Mono.empty();
                    })
                    .block();
        });

        assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);

    }

    @Test
    public void insertAll_shouldInsertAllDocuments() {
        Person customer1 = new Person(nextId(), "Dave");
        Person customer2 = new Person(nextId(), "James");

        reactiveTemplate.insertAll(asList(customer1, customer2)).blockLast();

        assertThat(findById(customer1.getId(), Person.class)).isEqualTo(customer1);
        assertThat(findById(customer2.getId(), Person.class)).isEqualTo(customer2);
    }

    @Test
    public void insertAll_rejectsDuplicateId() {
        Person person = new Person(id, "Amol");
        person.setAge(28);
        reactiveTemplate.insert(person).block();
        assertThat(findById(id, Person.class)).isEqualTo(person);

        StepVerifier.create(reactiveTemplate.insertAll(asList(person)))
                .expectError(DuplicateKeyException.class)
                .verify();
    }
}
