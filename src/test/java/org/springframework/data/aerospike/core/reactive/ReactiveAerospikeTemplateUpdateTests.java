package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.test.StepVerifier.create;

public class ReactiveAerospikeTemplateUpdateTests extends BaseReactiveIntegrationTests {

    @Test
    public void shouldThrowExceptionOnUpdateForNonexistingKey() {
        create(reactiveTemplate.update(new Person(id, "svenfirstName", 11)))
                .expectError(Exception.class)
                .verify();
    }

    @Test
    public void updatesEvenIfDocumentNotChanged() {
        Person person = new Person(id, "Wolfgan", 11);
        reactiveTemplate.insert(person).block();

        reactiveTemplate.update(person).block();

        Person result = findById(id, Person.class);

        assertThat(result.getAge()).isEqualTo(11);
    }

    @Test
    public void updatesMultipleFields() {
        Person person = new Person(id, null, 0);
        reactiveTemplate.insert(person).block();

        reactiveTemplate.update(new Person(id, "Andrew", 32)).block();

        assertThat(findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(32);
        });
    }

    @Test
    public void updatesFieldValueAndDocumentVersion() {
        VersionedClass document = new VersionedClass(id, "foobar");
        create(reactiveTemplate.insert(document))
                .assertNext(updated -> assertThat(updated.version).isEqualTo(1))
                .verifyComplete();
        assertThat(findById(id, VersionedClass.class).version).isEqualTo(1);

        document = new VersionedClass(id, "foobar1", document.version);
        create(reactiveTemplate.update(document))
                .assertNext(updated -> assertThat(updated.version).isEqualTo(2))
                .verifyComplete();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.field).isEqualTo("foobar1");
            assertThat(doc.version).isEqualTo(2);
        });

        document = new VersionedClass(id, "foobar2", document.version);
        create(reactiveTemplate.update(document))
                .assertNext(updated -> assertThat(updated.version).isEqualTo(3))
                .verifyComplete();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.field).isEqualTo("foobar2");
            assertThat(doc.version).isEqualTo(3);
        });
    }

    @Test
    public void updatesFieldToNull() {
        VersionedClass document = new VersionedClass(id, "foobar");
        reactiveTemplate.insert(document).block();

        document = new VersionedClass(id, null, document.version);
        reactiveTemplate.update(document).block();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.field).isNull();
            assertThat(doc.version).isEqualTo(2);
        });
    }

    @Test
    public void setsVersionEqualToNumberOfModifications() {
        VersionedClass document = new VersionedClass(id, "foobar");
        reactiveTemplate.insert(document).block();
        reactiveTemplate.update(document).block();
        reactiveTemplate.update(document).block();

        StepVerifier.create(reactorClient.get(new Policy(), new Key(getNameSpace(), "versioned-set", id)))
                .assertNext(keyRecord -> assertThat(keyRecord.record.generation).isEqualTo(3))
                .verifyComplete();
        VersionedClass actual = findById(id, VersionedClass.class);
        assertThat(actual.version).isEqualTo(3);
    }

    @Test
    public void onlyFirstUpdateSucceedsAndNextAttemptsShouldFailWithOptimisticLockingFailureExceptionForVersionedDocument() {
        VersionedClass document = new VersionedClass(id, "foobar");
        reactiveTemplate.insert(document).block();

        AtomicLong counter = new AtomicLong();
        AtomicLong optimisticLock = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            reactiveTemplate.update(new VersionedClass(id, data, document.version))
                    .onErrorResume(OptimisticLockingFailureException.class, (e) -> {
                        optimisticLock.incrementAndGet();
                        return Mono.empty();
                    })
                    .block();
        });

        assertThat(optimisticLock.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
    }

    @Test
    public void allConcurrentUpdatesSucceedForNonVersionedDocument() {
        Person document = new Person(id, "foobar");
        reactiveTemplate.insert(document).block();

        AtomicLong counter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String firstName = "value-" + counterValue;
            reactiveTemplate.update(new Person(id, firstName)).block();
        });

        Person actual = findById(id, Person.class);
        assertThat(actual.getFirstName()).startsWith("value-");
    }
}
