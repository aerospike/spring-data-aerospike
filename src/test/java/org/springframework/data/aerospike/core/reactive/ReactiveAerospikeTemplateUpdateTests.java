package org.springframework.data.aerospike.core.reactive;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.policy.Policy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.util.AsyncUtils;
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

    @BeforeEach
    public void beforeEach() {
        reactiveTemplate.deleteAll(Person.class).block();
        reactiveTemplate.deleteAll(VersionedClass.class).block();
    }

    @Test
    public void shouldThrowExceptionOnUpdateForNonExistingKey() {
        // RecordExistsAction.UPDATE_ONLY
        create(reactiveTemplate.update(new Person(id, "svenfirstName", 11)))
            .expectError(DataRetrievalFailureException.class)
            .verify();
    }

    @Test
    public void updatesEvenIfDocumentNotChanged() {
        Person person = new Person(id, "Wolfgang", 11);
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

        Person result = findById(id, Person.class);
        assertThat(result).satisfies(doc -> {
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

        Person result = findById(id, Person.class);
        assertThat(result).satisfies(doc -> {
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

        Person result = findById(id, Person.class);
        assertThat(result).satisfies(doc -> {
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

        Person result = findById(id, Person.class);
        assertThat(result).satisfies(doc -> {
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
            .assertNext(updated -> assertThat(updated.getVersion()).isEqualTo(1))
            .verifyComplete();
        assertThat(findById(id, VersionedClass.class).getVersion()).isEqualTo(1);

        document = new VersionedClass(id, "foobar1", document.getVersion());
        create(reactiveTemplate.update(document))
            .assertNext(updated -> assertThat(updated.getVersion()).isEqualTo(2))
            .verifyComplete();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar1");
            assertThat(doc.getVersion()).isEqualTo(2);
        });

        document = new VersionedClass(id, "foobar2", document.getVersion());
        create(reactiveTemplate.update(document))
            .assertNext(updated -> assertThat(updated.getVersion()).isEqualTo(3))
            .verifyComplete();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar2");
            assertThat(doc.getVersion()).isEqualTo(3);
        });
    }

    @Test
    public void updateSpecificFieldsWithDocumentVersion() {
        VersionedClass document = new VersionedClass(id, "foobar");
        reactiveTemplate.insert(document).block();
        assertThat(findById(id, VersionedClass.class).getVersion()).isEqualTo(1);

        document = new VersionedClass(id, "foobar1", document.getVersion());
        List<String> fields = new ArrayList<>();
        fields.add("field");
        reactiveTemplate.update(document, fields).block();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar1");
            assertThat(doc.getVersion()).isEqualTo(2);
        });

        document = new VersionedClass(id, "foobar2", document.getVersion());
        reactiveTemplate.update(document, fields).block();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar2");
            assertThat(doc.getVersion()).isEqualTo(3);
        });
    }

    @Test
    public void updatesFieldToNull() {
        VersionedClass document = new VersionedClass(id, "foobar");
        reactiveTemplate.insert(document).block();

        document = new VersionedClass(id, null, document.getVersion());
        reactiveTemplate.update(document).block();
        assertThat(findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.getField()).isNull();
            assertThat(doc.getVersion()).isEqualTo(2);
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
        assertThat(actual.getVersion()).isEqualTo(3);
    }

    @Test
    public void setsVersionEqualToNumberOfModificationsWithSetName() {
        VersionedClass document = new VersionedClass(id, "foobar");
        reactiveTemplate.insert(document, OVERRIDE_SET_NAME).block();
        reactiveTemplate.update(document, OVERRIDE_SET_NAME).block();
        reactiveTemplate.update(document, OVERRIDE_SET_NAME).block();

        StepVerifier.create(reactorClient.get(new Policy(), new Key(getNameSpace(), OVERRIDE_SET_NAME, id)))
            .assertNext(keyRecord -> assertThat(keyRecord.record.generation).isEqualTo(3))
            .verifyComplete();
        VersionedClass actual = findById(id, VersionedClass.class, OVERRIDE_SET_NAME);
        assertThat(actual.getVersion()).isEqualTo(3);
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
            reactiveTemplate.update(new VersionedClass(id, data, document.getVersion()))
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
            .stringMap(map)
            .strings(list)
            .build();

        reactiveTemplate.insert(person).block();

        Person personWithList = Person.builder().id(id).firstName("QLastName").age(50)
            .stringMap(map)
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
            .stringMap(map)
            .strings(list)
            .build();
        reactiveTemplate.insert(person).block();

        Person personWithList = Person.builder().id(id).firstName("QLastName").age(50)
            .stringMap(map)
            .strings(list)
            .build();
        personWithList.getStringMap().put("key4", "Added something new");

        List<String> fields = new ArrayList<>();
        fields.add("stringMap");
        reactiveTemplate.update(personWithList, fields).block();

        Person personWithList2 = findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStringMap()).hasSize(4);
        assertThat(personWithList2.getStringMap().get("key4")).isEqualTo("Added something new");
    }

    @Test
    public void TestAddToMapSpecifyingMapFieldOnlyWithSetName() {
        Map<String, String> map = new HashMap<>();
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        List<String> list = new ArrayList<>();
        list.add("string1");
        list.add("string2");
        list.add("string3");
        Person person = Person.builder().id(id).firstName("QLastName").age(50)
            .stringMap(map)
            .strings(list)
            .build();
        reactiveTemplate.insert(person, OVERRIDE_SET_NAME).block();

        Person personWithList = Person.builder().id(id).firstName("QLastName").age(50)
            .stringMap(map)
            .strings(list)
            .build();
        personWithList.getStringMap().put("key4", "Added something new");

        List<String> fields = new ArrayList<>();
        fields.add("stringMap");
        reactiveTemplate.update(personWithList, OVERRIDE_SET_NAME, fields).block();

        Person personWithList2 = findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStringMap()).hasSize(4);
        assertThat(personWithList2.getStringMap().get("key4")).isEqualTo("Added something new");
    }

    @Test
    public void updateAllIfDocumentsChanged() {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            int age1 = 140335200;
            int age2 = 177652800;
            Person person1 = new Person(id, "Wolfgang", age1);
            Person person2 = new Person(nextId(), "Johann", age2);
            reactiveTemplate.insertAll(List.of(person1, person2)).blockLast();

            person1.setFirstName("Wolfgang M");
            person2.setFirstName("Johann B");
            reactiveTemplate.updateAll(List.of(person1, person2)).blockLast();

            Person result1 = reactiveTemplate.findById(person1.getId(), Person.class).block();
            Person result2 = reactiveTemplate.findById(person2.getId(), Person.class).block();
            assertThat(result1.getAge()).isEqualTo(age1);
            assertThat(result1.getFirstName()).isEqualTo("Wolfgang M");
            assertThat(result2.getAge()).isEqualTo(age2);
            assertThat(result2.getFirstName()).isEqualTo("Johann B");

            List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
            Iterable<Person> personsWithUpdate = persons.stream()
                .peek(person -> person.setFirstName(person.getFirstName() + "_")).toList();

            reactiveTemplate.updateAll(personsWithUpdate).blockLast();
            personsWithUpdate.forEach(person ->
                assertThat(reactiveTemplate.findById(person.getId(), Person.class).block().getFirstName()
                    .equals(person.getFirstName())).isTrue());
        }
    }

    @Test
    public void updateAllShouldThrowExceptionOnUpdateForNonExistingKey() {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            Person person1 = new Person(id, "svenfirstName", 11);
            Person person2 = new Person(nextId(), "svenfirstName", 11);
            Person person3 = new Person(nextId(), "svenfirstName", 11);
            reactiveTemplate.save(person3).block();
            // RecordExistsAction.UPDATE_ONLY
            assertThatThrownBy(() -> reactiveTemplate.updateAll(List.of(person1, person2)).blockLast())
                .isInstanceOf(AerospikeException.BatchRecordArray.class);

            assertThat(reactiveTemplate.findById(person1.getId(), Person.class).block()).isNull();
            assertThat(reactiveTemplate.findById(person2.getId(), Person.class).block()).isNull();
            assertThat(reactiveTemplate.findById(person3.getId(), Person.class).block()).isEqualTo(person3);
        }
    }

    @Test
    public void updateAllIfDocumentsNotChanged() {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            int age1 = 140335200;
            int age2 = 177652800;
            Person person1 = new Person(id, "Wolfgang", age1);
            Person person2 = new Person(nextId(), "Johann", age2);
            reactiveTemplate.insertAll(List.of(person1, person2)).blockLast();
            reactiveTemplate.updateAll(List.of(person1, person2)).blockLast();

            Person result1 = reactiveTemplate.findById(person1.getId(), Person.class).block();
            Person result2 = reactiveTemplate.findById(person2.getId(), Person.class).block();
            assertThat(result1.getAge()).isEqualTo(age1);
            assertThat(result2.getAge()).isEqualTo(age2);
        }
    }
}
