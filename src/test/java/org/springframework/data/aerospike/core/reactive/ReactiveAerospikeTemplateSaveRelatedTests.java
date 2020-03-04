package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import org.junit.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
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
        reactiveTemplate.save(first).block();

        assertThat(first.version).isEqualTo(1);
        assertThat(findById(id, VersionedClass.class).version).isEqualTo(1);
    }

    @Test(expected = OptimisticLockingFailureException.class)
    public void save_shouldNotSaveDocumentIfItAlreadyExistsWithZeroVersion() {
        reactiveTemplate.save(new VersionedClass(id, "foo", 0L)).block();
        reactiveTemplate.save(new VersionedClass(id, "foo", 0L)).block();
    }

    @Test
    public void save_shouldSaveDocumentWithEqualVersion() {
        reactiveTemplate.save(new VersionedClass(id, "foo")).block();

        reactiveTemplate.save(new VersionedClass(id, "foo", 1L)).block();
        reactiveTemplate.save(new VersionedClass(id, "foo", 2L)).block();
    }

    @Test(expected = DataRetrievalFailureException.class)
    public void save_shouldFailSaveNewDocumentWithVersionGreaterThanZero() {
        reactiveTemplate.save(new VersionedClass(id, "foo", 5L)).block();
    }

    @Test
    public void save_shouldUpdateNullField() {
        VersionedClass versionedClass = new VersionedClass(id, null);
        VersionedClass saved = reactiveTemplate.save(versionedClass).block();
        reactiveTemplate.save(saved).block();
    }

    @Test
    public void save_shouldUpdateNullFieldForClassWithVersionField() {
        VersionedClass versionedClass = new VersionedClass(id, "field");
        reactiveTemplate.save(versionedClass).block();

        assertThat(findById(id, VersionedClass.class).getField()).isEqualTo("field");

        versionedClass.setField(null);
        reactiveTemplate.save(versionedClass).block();

        assertThat(findById(id, VersionedClass.class).getField()).isNull();
    }

    @Test
    public void save_shouldUpdateNullFieldForClassWithoutVersionField() {
        Person person = new Person(id, "Oliver");
        reactiveTemplate.save(person).block();

        assertThat(findById(id, Person.class).getFirstName()).isEqualTo("Oliver");

        person.setFirstName(null);
        reactiveTemplate.save(person).block();

        assertThat(findById(id, Person.class).getFirstName()).isNull();
    }

    @Test
    public void save_shouldUpdateExistingDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        reactiveTemplate.save(one).block();

        reactiveTemplate.save(new VersionedClass(id, "foo1", one.version)).block();

        VersionedClass value = findById(id, VersionedClass.class);
        assertThat(value.version).isEqualTo(2);
        assertThat(value.field).isEqualTo("foo1");
    }

    @Test
    public void save_shouldSetVersionWhenSavingTheSameDocument() {
        VersionedClass one = new VersionedClass(id, "foo");
        reactiveTemplate.save(one).block();
        reactiveTemplate.save(one).block();
        reactiveTemplate.save(one).block();

        assertThat(one.version).isEqualTo(3);
    }

    @Test
    public void save_shouldUpdateAlreadyExistingDocument() throws Exception {
        AtomicLong counter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        VersionedClass initial = new VersionedClass(id, "value-0");
        reactiveTemplate.save(initial).block();
        assertThat(initial.version).isEqualTo(1);

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            boolean saved = false;
            while (!saved) {
                long counterValue = counter.incrementAndGet();
                VersionedClass messageData = findById(id, VersionedClass.class);
                messageData.field = "value-" + counterValue;
                try {
                    reactiveTemplate.save(messageData).block();
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
    public void save_shouldSaveOnlyFirstDocumentAndNextAttemptsShouldFailWithOptimisticLockingException() throws Exception {
        AtomicLong counter = new AtomicLong();
        AtomicLong optimisticLockCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            VersionedClass messageData = new VersionedClass(id, data);
            reactiveTemplate.save(messageData)
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

        reactiveTemplate.save(one).block();
        reactiveTemplate.save(one).block();

        assertThat(findById(id, CustomCollectionClass.class)).isEqualTo(one);
    }

    @Test
    public void save_shouldUpdateDocumentDataWithoutVersion() {
        CustomCollectionClass first = new CustomCollectionClass(id, "numbers");
        CustomCollectionClass second = new CustomCollectionClass(id, "hot dog");

        reactiveTemplate.save(first).block();
        reactiveTemplate.save(second).block();

        assertThat(findById(id, CustomCollectionClass.class)).isEqualTo(second);
    }

    @Test
    public void save_shouldReplaceAllBinsPresentInAerospikeWhenSavingDocument() {
        Key key = new Key(getNameSpace(), "versioned-set", id);
        VersionedClass first = new VersionedClass(id, "foo");
        reactiveTemplate.save(first).block();
        blockingAerospikeTestOperations.addNewFieldToSavedDataInAerospike(key);

        reactiveTemplate.save(new VersionedClass(id, "foo2", 2L)).block();

        StepVerifier.create(reactorClient.get(new Policy(), key))
                .assertNext(keyRecord -> {
                    assertThat(keyRecord.record.bins)
                            .doesNotContainKey("notPresent")
                            .contains(entry("field", "foo2"));
                })
                .verifyComplete();

    }

    @Test(expected = IllegalArgumentException.class)
    public void save_rejectsNullObjectToBeSaved() {
        reactiveTemplate.save(null).block();
    }

}
