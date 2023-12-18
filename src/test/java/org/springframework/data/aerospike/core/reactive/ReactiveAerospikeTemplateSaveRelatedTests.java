package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.utility.AsyncUtils;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

/**
 * Tests for save related methods in {@link ReactiveAerospikeTemplate}.
 *
 * @author Igor Ermolenko
 */
@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class ReactiveAerospikeTemplateSaveRelatedTests extends BaseReactiveIntegrationTests {

    @Test
    public void save_shouldSaveAndSetVersion() {
        VersionedClass first = new VersionedClass(id, "foo");
        reactiveTemplate.save(first).subscribeOn(Schedulers.parallel()).block();

        assertThat(first.getVersion()).isEqualTo(1);
        assertThat(findById(id, VersionedClass.class).getVersion()).isEqualTo(1);
    }

    @Test
    public void saveWithSetName_shouldSaveAndSetVersion() {
        VersionedClass first = new VersionedClass(id, "foo");
        reactiveTemplate.save(first, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel()).block();

        assertThat(first.getVersion()).isEqualTo(1);
        assertThat(findById(id, VersionedClass.class, OVERRIDE_SET_NAME).getVersion()).isEqualTo(1);
    }

    @Test
    public void save_shouldNotSaveDocumentIfItAlreadyExistsWithZeroVersion() {
        reactiveTemplate.save(new VersionedClass(id, "foo", 0L))
            .subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(reactiveTemplate.save(new VersionedClass(id, "foo", 0L))
                .subscribeOn(Schedulers.parallel()))
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
        StepVerifier.create(reactiveTemplate.save(new VersionedClass(id, "foo", 5L))
                .subscribeOn(Schedulers.parallel()))
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
    public void saveWithSetName_shouldUpdateNullField() {
        VersionedClass versionedClass = new VersionedClass(id, null);
        VersionedClass saved = reactiveTemplate.save(versionedClass, OVERRIDE_SET_NAME)
            .subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(saved, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel()).block();
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

        Person result = findById(id, Person.class);
        assertThat(result.getFirstName()).isNull();
        reactiveTemplate.delete(result).block(); // cleanup
    }

    @Test
    public void saveWithSetName_shouldUpdateNullFieldForClassWithoutVersionField() {
        Person person = new Person(id, "Oliver");
        reactiveTemplate.save(person, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel()).block();

        assertThat(findById(id, Person.class, OVERRIDE_SET_NAME).getFirstName()).isEqualTo("Oliver");

        person.setFirstName(null);
        reactiveTemplate.save(person, OVERRIDE_SET_NAME).subscribeOn(Schedulers.parallel()).block();

        Person result = findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(result.getFirstName()).isNull();
        reactiveTemplate.delete(result, OVERRIDE_SET_NAME).block(); // cleanup
    }

    @Test
    public void save_shouldUpdateExistingDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();

        reactiveTemplate.save(new VersionedClass(id, "foo1", one.getVersion()))
            .subscribeOn(Schedulers.parallel()).block();

        VersionedClass value = findById(id, VersionedClass.class);
        assertThat(value.getVersion()).isEqualTo(2);
        assertThat(value.getField()).isEqualTo("foo1");
    }

    @Test
    public void save_shouldSetVersionWhenSavingTheSameDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();
        reactiveTemplate.save(one).subscribeOn(Schedulers.parallel()).block();

        assertThat(one.getVersion()).isEqualTo(3);
    }

    @Test
    public void save_shouldUpdateAlreadyExistingDocument() {
        AtomicLong counter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        VersionedClass initial = new VersionedClass(id, "value-0");
        reactiveTemplate.save(initial).subscribeOn(Schedulers.parallel()).block();
        assertThat(initial.getVersion()).isEqualTo(1);

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            boolean saved = false;
            while (!saved) {
                long counterValue = counter.incrementAndGet();
                VersionedClass messageData = findById(id, VersionedClass.class);
                messageData.setField("value-" + counterValue);
                try {
                    reactiveTemplate.save(messageData).subscribeOn(Schedulers.parallel()).block();
                    saved = true;
                } catch (OptimisticLockingFailureException ignore) {
                }
            }
        });

        VersionedClass actual = findById(id, VersionedClass.class);

        assertThat(actual.getField()).isNotEqualTo(initial.getField());
        assertThat(actual.getVersion()).isNotEqualTo(initial.getVersion())
            .isEqualTo(initial.getVersion() + numberOfConcurrentSaves);
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

        reactiveTemplate.save(new VersionedClass(id, "foo2", 2L))
            .subscribeOn(Schedulers.parallel()).block();

        StepVerifier.create(reactorClient.get(new Policy(), key))
            .assertNext(keyRecord -> assertThat(keyRecord.record.bins)
                .doesNotContainKey("notPresent")
                .contains(entry("field", "foo2")))
            .verifyComplete();
    }

    @Test
    public void save_rejectsNullObjectToBeSaved() {
        assertThatThrownBy(() -> reactiveTemplate.save(null).block())
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void saveAll_shouldSaveAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionUtils.isBatchWriteSupported()) {
            Person customer1 = new Person(nextId(), "Dave");
            Person customer2 = new Person(nextId(), "James");
            reactiveTemplate.saveAll(List.of(customer1, customer2)).blockLast();

            Person result1 = findById(customer1.getId(), Person.class);
            Person result2 = findById(customer2.getId(), Person.class);
            assertThat(result1).isEqualTo(customer1);
            assertThat(result2).isEqualTo(customer2);
            reactiveTemplate.delete(result1).block(); // cleanup
            reactiveTemplate.delete(result2).block(); // cleanup
        }
    }

    @Test
    public void saveAllWithSetName_shouldSaveAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionUtils.isBatchWriteSupported()) {
            Person customer1 = new Person(nextId(), "Dave");
            Person customer2 = new Person(nextId(), "James");
            reactiveTemplate.saveAll(List.of(customer1, customer2), OVERRIDE_SET_NAME).blockLast();

            Person result1 = findById(customer1.getId(), Person.class, OVERRIDE_SET_NAME);
            Person result2 = findById(customer2.getId(), Person.class, OVERRIDE_SET_NAME);
            assertThat(result1).isEqualTo(customer1);
            assertThat(result2).isEqualTo(customer2);
            reactiveTemplate.delete(result1, OVERRIDE_SET_NAME).block(); // cleanup
            reactiveTemplate.delete(result2, OVERRIDE_SET_NAME).block(); // cleanup
        }
    }

    @Test
    public void saveAll_rejectsDuplicateId() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionUtils.isBatchWriteSupported()) {
            VersionedClass first = new VersionedClass(id, "foo");

            StepVerifier.create(reactiveTemplate.saveAll(List.of(first, first)))
                .expectError(AerospikeException.BatchRecordArray.class)
                .verify();
            reactiveTemplate.delete(findById(id, VersionedClass.class)).block(); // cleanup
        }
    }

    @Test
    public void saveAllWithSetName_rejectsDuplicateId() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionUtils.isBatchWriteSupported()) {
            VersionedClass first = new VersionedClass(id, "foo");

            StepVerifier.create(reactiveTemplate.saveAll(List.of(first, first), OVERRIDE_SET_NAME))
                .expectError(AerospikeException.BatchRecordArray.class)
                .verify();
            reactiveTemplate.delete(findById(id, VersionedClass.class, OVERRIDE_SET_NAME), OVERRIDE_SET_NAME)
                .block(); // cleanup
        }
    }
}
