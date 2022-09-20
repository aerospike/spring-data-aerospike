package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.sample.Person;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static reactor.test.StepVerifier.create;

public class ReactiveAerospikeTemplateUpdateTests extends BaseReactiveIntegrationTests {

    @Test
    public void shouldThrowExceptionOnUpdateForNonexistingKey() {
        create(reactiveTemplate.update(new Person(id, "svenfirstName", 11)))
                .expectError(DataRetrievalFailureException.class)
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
    public void updateSpecificFields() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20).build();
        reactiveTemplate.insert(person).block();

        List<String> fields = new ArrayList<>();
        fields.add("age");
        reactiveTemplate.update(Person.builder().id(id).age(41).build(), fields).block();

        assertThat(findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(41);
            assertThat(doc.getWaist()).isEqualTo(20);
        });
    }

    @Test
    public void shouldFailUpdateNonExistingSpecificField() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20).build();
        reactiveTemplate.insert(person).block();

        List<String> fields = new ArrayList<>();
        fields.add("age");
        fields.add("non-existing-field");

        assertThatThrownBy(() -> reactiveTemplate.update(Person.builder().id(id).age(41).build(), fields).block())
                .isInstanceOf(RecoverableDataAccessException.class)
                .hasMessageContaining("field doesn't exists");
    }

    @Test
    public void updateSpecificFieldsWithFieldAnnotatedProperty() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20)
                .emailAddress("andrew@gmail.com").build();
        reactiveTemplate.insert(person).block();

        List<String> fields = new ArrayList<>();
        fields.add("age");
        fields.add("emailAddress");
        reactiveTemplate.update(Person.builder().id(id).age(41).emailAddress("andrew2@gmail.com").build(), fields)
                .block();

        assertThat(findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(41);
            assertThat(doc.getWaist()).isEqualTo(20);
            assertThat(doc.getEmailAddress()).isEqualTo("andrew2@gmail.com");
        });
    }

    @Test
    public void updateSpecificFieldsWithFieldAnnotatedPropertyActualValue() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20)
                .emailAddress("andrew@gmail.com").build();
        reactiveTemplate.insert(person).block();

        List<String> fields = new ArrayList<>();
        fields.add("age");
        fields.add("email");
        reactiveTemplate.update(Person.builder().id(id).age(41).emailAddress("andrew2@gmail.com").build(), fields)
                .block();

        assertThat(findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(41);
            assertThat(doc.getWaist()).isEqualTo(20);
            assertThat(doc.getEmailAddress()).isEqualTo("andrew2@gmail.com");
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
    public void updateSpecificFieldsWithDocumentVersion() {
        VersionedClass document = new VersionedClass(id, "foobar");
        reactiveTemplate.insert(document).block();
        assertThat(findById(id, VersionedClass.class).version).isEqualTo(1);

        document = new VersionedClass(id, "foobar1", document.version);
        List<String> fields = new ArrayList<>();
        fields.add("field");
        reactiveTemplate.update(document, fields).block();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.field).isEqualTo("foobar1");
            assertThat(doc.version).isEqualTo(2);
        });

        document = new VersionedClass(id, "foobar2", document.version);
        reactiveTemplate.update(document, fields).block();
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

    @Test
    public void TestAddToListSpecifyingListFieldOnly() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        List<String> list = new ArrayList<>();
        list.add("string1");
        list.add("string2");
        list.add("string3");
        Person person = Person.builder().id(id).firstName("QLastName").age(50)
                .map(map)
                .strings(list)
                .build();

        reactiveTemplate.insert(person).block();

        Person personWithList = Person.builder().id(id).firstName("QLastName").age(50)
                .map(map)
                .strings(list)
                .build();
        personWithList.getStrings().add("Added something new");

        List<String> fields = new ArrayList<>();
        fields.add("strings");
        reactiveTemplate.update(personWithList, fields).block();

        Person personWithList2 = findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStrings()).hasSize(4);
    }

    @Test
    public void TestAddToMapSpecifyingMapFieldOnly() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        List<String> list = new ArrayList<>();
        list.add("string1");
        list.add("string2");
        list.add("string3");
        Person person = Person.builder().id(id).firstName("QLastName").age(50)
                .map(map)
                .strings(list)
                .build();
        reactiveTemplate.insert(person).block();

        Person personWithList = Person.builder().id(id).firstName("QLastName").age(50)
                .map(map)
                .strings(list)
                .build();
        personWithList.getMap().put("key4", "Added something new");

        List<String> fields = new ArrayList<>();
        fields.add("map");
        reactiveTemplate.update(personWithList, fields).block();

        Person personWithList2 = findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getMap()).hasSize(4);
        assertThat(personWithList2.getMap().get("key4")).isEqualTo("Added something new");
    }
}
