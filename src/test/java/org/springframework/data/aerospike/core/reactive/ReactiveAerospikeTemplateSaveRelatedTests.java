package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Tests for save related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeTemplateSaveRelatedTests extends BaseReactiveIntegrationTests {

    @Test
    public void save_shouldSaveAndSetVersion() {
        VersionedClass first = new VersionedClass(id, "foo");
        reactiveTemplate.save(first).subscribeOn(Schedulers.parallel()).block();

        assertThat(first.version).isEqualTo(1);
        assertThat(findById(id, VersionedClass.class).version).isEqualTo(1);
    }

    @Test
    public void save_shouldNotSaveDocumentIfItAlreadyExistsWithZeroVersion() {
        reactiveTemplate.save(new VersionedClass(id, "foo", 0L)).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(reactiveTemplate.save(new VersionedClass(id, "foo", 0L)).subscribeOn(Schedulers.parallel()))
                .expectError(OptimisticLockingFailureException.class)
                .verify();
    }

    @Test
    public void save_shouldSaveDocumentWithEqualVersion() {
        reactiveTemplate.save(new VersionedClass(id, "foo")).subscribeOn(Schedulers.parallel()).block();

        reactiveTemplate.save(new VersionedClass(id, "foo", 1L)).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(new VersionedClass(id, "foo", 2L)).subscribeOn(Schedulers.parallel()).block();
    }

    @Test
    public void save_shouldFailSaveNewDocumentWithVersionGreaterThanZero() {
        StepVerifier.create(reactiveTemplate.save(new VersionedClass(id, "foo", 5L)).subscribeOn(Schedulers.parallel()))
                .expectError(DataRetrievalFailureException.class)
                .verify();
    }

    @Test
    public void save_shouldUpdateNullField() {
        VersionedClass versionedClass = new VersionedClass(id, null);
        VersionedClass saved = reactiveTemplate.save(versionedClass).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(saved).subscribeOn(Schedulers.parallel()).block();
    }

    @Test
    public void save_shouldUpdateNullFieldForClassWithVersionField() {
        VersionedClass versionedClass = new VersionedClass(id, "field");
        reactiveTemplate.save(versionedClass).subscribeOn(Schedulers.parallel()).block();

        assertThat(findById(id, VersionedClass.class).getField()).isEqualTo("field");

        versionedClass.setField(null);
        reactiveTemplate.save(versionedClass).subscribeOn(Schedulers.parallel()).block();

        assertThat(findById(id, VersionedClass.class).getField()).isNull();
    }

    @Test
    public void save_shouldUpdateNullFieldForClassWithoutVersionField() {
        Person person = new Person(id, "Oliver");
        reactiveTemplate.save(person).subscribeOn(Schedulers.parallel()).block();

        assertThat(findById(id, Person.class).getFirstName()).isEqualTo("Oliver");

        person.setFirstName(null);
        reactiveTemplate.save(person).subscribeOn(Schedulers.parallel()).block();

        assertThat(findById(id, Person.class).getFirstName()).isNull();
    }

    @Test
    public void save_shouldUpdateExistingDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();

        reactiveTemplate.save(new VersionedClass(id, "foo1", one.version)).subscribeOn(Schedulers.parallel()).block();

        VersionedClass value = findById(id, VersionedClass.class);
        assertThat(value.version).isEqualTo(2);
        assertThat(value.field).isEqualTo("foo1");
    }

    @Test
    public void save_shouldSetVersionWhenSavingTheSameDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();

        assertThat(one.version).isEqualTo(3);
    }

    @Test
    public void save_shouldUpdateAlreadyExistingDocument() {
        AtomicLong counter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        VersionedClass initial = new VersionedClass(id, "value-0");
        reactiveTemplate.save(initial).subscribeOn(Schedulers.parallel()).block();
        assertThat(initial.version).isEqualTo(1);

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            boolean saved = false;
            while (!saved) {
                long counterValue = counter.incrementAndGet();
                VersionedClass messageData = findById(id, VersionedClass.class);
                messageData.field = "value-" + counterValue;
                try {
                    reactiveTemplate.save(messageData).subscribeOn(Schedulers.parallel()).block();
                    saved = true;
                } catch (OptimisticLockingFailureException ignore) {
                }
            }
        });

        VersionedClass actual = findById(id, VersionedClass.class);

        assertThat(actual.field).isNotEqualTo(initial.field);
        assertThat(actual.version).isNotEqualTo(initial.version);
        assertThat(actual.version).isEqualTo(initial.version + numberOfConcurrentSaves);
    }

    @Test
    public void save_shouldSaveOnlyFirstDocumentAndNextAttemptsShouldFailWithOptimisticLockingException() {
        AtomicLong counter = new AtomicLong();
        AtomicLong optimisticLockCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            VersionedClass messageData = new VersionedClass(id, data);
            reactiveTemplate.save(messageData)
                    .subscribeOn(Schedulers.parallel())
                    .onErrorResume(OptimisticLockingFailureException.class, (e) -> {
                        optimisticLockCounter.incrementAndGet();
                        return Mono.empty();
                    })
                    .block();
        });

        assertThat(optimisticLockCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
    }

    @Test
    public void save_shouldSaveMultipleTimeDocumentWithoutVersion() {
        CustomCollectionClass one = new CustomCollectionClass(id, "numbers");

        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();

        assertThat(findById(id, CustomCollectionClass.class)).isEqualTo(one);
    }

    @Test
    public void save_shouldUpdateDocumentDataWithoutVersion() {
        CustomCollectionClass first = new CustomCollectionClass(id, "numbers");
        CustomCollectionClass second = new CustomCollectionClass(id, "hot dog");

        reactiveTemplate.save(first).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(second).subscribeOn(Schedulers.parallel()).block();

        assertThat(findById(id, CustomCollectionClass.class)).isEqualTo(second);
    }

    @Test
    public void save_shouldReplaceAllBinsPresentInAerospikeWhenSavingDocument() {
        Key key = new Key(getNameSpace(), "versioned-set", id);
        VersionedClass first = new VersionedClass(id, "foo");
        reactiveTemplate.save(first).subscribeOn(Schedulers.parallel()).block();
        additionalAerospikeTestOperations.addNewFieldToSavedDataInAerospike(key);

        reactiveTemplate.save(new VersionedClass(id, "foo2", 2L)).subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(reactorClient.get(new Policy(), key))
                .assertNext(keyRecord -> {
                    assertThat(keyRecord.record.bins)
                            .doesNotContainKey("notPresent")
                            .contains(entry("field", "foo2"));
                })
                .verifyComplete();

    }

    @Test
    public void save_rejectsNullObjectToBeSaved() {
        assertThatThrownBy(() -> reactiveTemplate.save(null).block())
                .isInstanceOf(IllegalArgumentException.class);
    }

}
