/*
 * Copyright 2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.AsyncUtils;
import org.springframework.data.aerospike.utility.ServerVersionUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;
import static org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class AerospikeTemplateUpdateTests extends BaseBlockingIntegrationTests {

    @Test
    public void shouldThrowExceptionOnUpdateForNonExistingKey() {
        // RecordExistsAction.UPDATE_ONLY
        assertThatThrownBy(() -> template.update(new Person(id, "svenfirstName", 11)))
            .isInstanceOf(DataRetrievalFailureException.class);
    }

    @Test
    public void updatesEvenIfDocumentNotChanged() {
        Person person = new Person(id, "Wolfgang", 11);
        template.insert(person);
        template.update(person);

        Person result = template.findById(id, Person.class);
        assertThat(result.getAge()).isEqualTo(11);
        template.delete(result); // cleanup
    }

    @Test
    public void updatesMultipleFields() {
        Person person = new Person(id, null, 0);
        template.insert(person);
        template.update(new Person(id, "Andrew", 32));

        assertThat(template.findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(32);
        });
        template.delete(template.findById(id, Person.class)); // cleanup
    }

    @Test
    public void updateSpecificFields() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20).build();
        template.insert(person);

        List<String> fields = new ArrayList<>();
        fields.add("age");
        template.update(Person.builder().id(id).age(41).build(), fields);

        assertThat(template.findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(41);
            assertThat(doc.getWaist()).isEqualTo(20);
        });
        template.delete(template.findById(id, Person.class)); // cleanup
    }

    @Test
    public void shouldFailUpdateNonExistingSpecificField() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20).build();
        template.insert(person);

        List<String> fields = new ArrayList<>();
        fields.add("age");
        fields.add("non-existing-field");

        assertThatThrownBy(() -> template.update(Person.builder().id(id).age(41).build(), fields))
            .isInstanceOf(RecoverableDataAccessException.class)
            .hasMessageContaining("field doesn't exists");
        template.delete(template.findById(id, Person.class)); // cleanup
    }

    @Test
    public void updateSpecificFieldsWithFieldAnnotatedProperty() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20)
            .emailAddress("andrew@gmail.com").build();
        template.insert(person);

        List<String> fields = new ArrayList<>();
        fields.add("age");
        fields.add("emailAddress");
        template.update(Person.builder().id(id).age(41).emailAddress("andrew2@gmail.com").build(), fields);

        assertThat(template.findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(41);
            assertThat(doc.getWaist()).isEqualTo(20);
            assertThat(doc.getEmailAddress()).isEqualTo("andrew2@gmail.com");
        });
        template.delete(template.findById(id, Person.class)); // cleanup
    }

    @Test
    public void updateSpecificFieldsWithFieldAnnotatedPropertyAndSetName() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20)
            .emailAddress("andrew@gmail.com").build();
        template.insert(person, OVERRIDE_SET_NAME);

        List<String> fields = new ArrayList<>();
        fields.add("age");
        fields.add("emailAddress");
        template.update(Person.builder().id(id).age(41).emailAddress("andrew2@gmail.com")
            .build(), OVERRIDE_SET_NAME, fields);

        assertThat(template.findById(id, Person.class, OVERRIDE_SET_NAME)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(41);
            assertThat(doc.getWaist()).isEqualTo(20);
            assertThat(doc.getEmailAddress()).isEqualTo("andrew2@gmail.com");
        });
        template.delete(template.findById(id, Person.class, OVERRIDE_SET_NAME), OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void updateSpecificFieldsWithFieldAnnotatedPropertyActualValue() {
        Person person = Person.builder().id(id).firstName("Andrew").lastName("Yo").age(40).waist(20)
            .emailAddress("andrew@gmail.com").build();
        template.insert(person);

        List<String> fields = new ArrayList<>();
        fields.add("age");
        fields.add("email");
        template.update(Person.builder().id(id).age(41).emailAddress("andrew2@gmail.com").build(), fields);

        assertThat(template.findById(id, Person.class)).satisfies(doc -> {
            assertThat(doc.getFirstName()).isEqualTo("Andrew");
            assertThat(doc.getAge()).isEqualTo(41);
            assertThat(doc.getWaist()).isEqualTo(20);
            assertThat(doc.getEmailAddress()).isEqualTo("andrew2@gmail.com");
        });
        template.delete(template.findById(id, Person.class)); // cleanup
    }

    @Test
    public void updatesFieldValueAndDocumentVersion() {
        VersionedClass document = new VersionedClass(id, "foobar");
        template.insert(document);
        assertThat(template.findById(id, VersionedClass.class).getVersion()).isEqualTo(1);

        document = new VersionedClass(id, "foobar1", document.getVersion());
        template.update(document);
        assertThat(template.findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar1");
            assertThat(doc.getVersion()).isEqualTo(2);
        });

        document = new VersionedClass(id, "foobar2", document.getVersion());
        template.update(document);
        VersionedClass result = template.findById(id, VersionedClass.class);
        assertThat(result).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar2");
            assertThat(doc.getVersion()).isEqualTo(3);
        });
        template.delete(result); // cleanup
    }

    @Test
    public void updateSpecificFieldsWithDocumentVersion() {
        VersionedClass document = new VersionedClass(id, "foobar");
        template.insert(document);
        assertThat(template.findById(id, VersionedClass.class).getVersion()).isEqualTo(1);

        document = new VersionedClass(id, "foobar1", document.getVersion());
        List<String> fields = new ArrayList<>();
        fields.add("field");
        template.update(document, fields);
        assertThat(template.findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar1");
            assertThat(doc.getVersion()).isEqualTo(2);
        });

        document = new VersionedClass(id, "foobar2", document.getVersion());
        template.update(document, fields);
        VersionedClass result = template.findById(id, VersionedClass.class);
        assertThat(result).satisfies(doc -> {
            assertThat(doc.getField()).isEqualTo("foobar2");
            assertThat(doc.getVersion()).isEqualTo(3);
        });
        template.delete(result); // cleanup
    }

    @Test
    public void updatesFieldToNull() {
        VersionedClass document = new VersionedClass(id, "foobar");
        template.insert(document);

        document = new VersionedClass(id, null, document.getVersion());
        template.update(document);
        VersionedClass result = template.findById(id, VersionedClass.class);
        assertThat(result).satisfies(doc -> {
            assertThat(doc.getField()).isNull();
            assertThat(doc.getVersion()).isEqualTo(2);
        });
        template.delete(result); // cleanup
    }

    @Test
    public void setsVersionEqualToNumberOfModifications() {
        VersionedClass document = new VersionedClass(id, "foobar");
        template.insert(document);
        template.update(document);
        template.update(document);

        Record raw = client.get(new Policy(), new Key(getNameSpace(), "versioned-set", id));
        assertThat(raw.generation).isEqualTo(3);
        VersionedClass actual = template.findById(id, VersionedClass.class);
        assertThat(actual.getVersion()).isEqualTo(3);
        template.delete(actual); // cleanup
    }

    @Test
    public void onlyFirstUpdateSucceedsAndNextAttemptsShouldFailWithOptimisticLockingFailureExceptionForVersionedDocument() {
        VersionedClass document = new VersionedClass(id, "foobar");
        template.insert(document);

        AtomicLong counter = new AtomicLong();
        AtomicLong optimisticLock = new AtomicLong();
        int numberOfConcurrentSaves = 5;
        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            try {
                template.update(new VersionedClass(id, data, document.getVersion()));
            } catch (OptimisticLockingFailureException e) {
                optimisticLock.incrementAndGet();
            }
        });

        assertThat(optimisticLock.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
        template.delete(template.findById(id, VersionedClass.class)); // cleanup
    }

    @Test
    public void allConcurrentUpdatesSucceedForNonVersionedDocument() {
        Person document = new Person(id, "foobar");
        template.insert(document);

        AtomicLong counter = new AtomicLong();
        int numberOfConcurrentSaves = 5;
        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String firstName = "value-" + counterValue;
            template.update(new Person(id, firstName));
        });

        Person actual = template.findById(id, Person.class);
        assertThat(actual.getFirstName()).startsWith("value-");
        template.delete(actual); // cleanup
    }

    @Test
    public void TestAddToList() {
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
        template.insert(person);

        Person personWithList = template.findById(id, Person.class);
        personWithList.getStrings().add("Added something new");
        template.update(personWithList);

        Person personWithList2 = template.findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStrings()).hasSize(4);
        template.delete(personWithList2); // cleanup
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
        template.insert(person);

        Person personWithList = Person.builder().id(id).firstName("QLastName").age(50)
            .stringMap(map)
            .strings(list)
            .build();
        personWithList.getStrings().add("Added something new");

        List<String> fields = new ArrayList<>();
        fields.add("strings");
        template.update(personWithList, fields);

        Person personWithList2 = template.findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStrings()).hasSize(4);
        template.delete(personWithList2); // cleanup
    }

    @Test
    public void TestAddToMap() {
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
        template.insert(person);

        Person personWithList = template.findById(id, Person.class);
        personWithList.getStringMap().put("key4", "Added something new");
        template.update(personWithList);

        Person personWithList2 = template.findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStringMap()).hasSize(4);
        assertThat(personWithList2.getStringMap().get("key4")).isEqualTo("Added something new");
        template.delete(personWithList2); // cleanup
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
        template.insert(person);

        Person personWithList = Person.builder().id(id).firstName("QLastName").age(50)
            .stringMap(map)
            .strings(list)
            .build();
        personWithList.getStringMap().put("key4", "Added something new");

        List<String> fields = new ArrayList<>();
        fields.add("stringMap");
        template.update(personWithList, fields);

        Person personWithList2 = template.findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStringMap()).hasSize(4);
        assertThat(personWithList2.getStringMap().get("key4")).isEqualTo("Added something new");
        template.delete(personWithList2); // cleanup
    }

    @Test
    public void updateAllShouldThrowExceptionOnUpdateForNonExistingKey() {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            VersionedClass first = new VersionedClass("newId1", "foo");  // This class has a version field (class
            // field annotated with @Version). The constructor does not receive the version, so it stays equal to zero
            VersionedClass second = new VersionedClass("newId2", "bar"); //
            assertThat(first.getVersion() == 0).isTrue(); // The document's version is zero meaning there is no
            // corresponding DB record
            assertThat(second.getVersion() == 0).isTrue();
            template.insert(first);
            assertThat(first.getVersion() == 1).isTrue(); // The document's version is equal to one meaning there is
            // a corresponding DB record
            // RecordExistsAction.UPDATE_ONLY
            assertThatThrownBy(() -> template.updateAll(List.of(first, second))) // An attempt to update versioned
                // documents without already existing DB records results in getting BatchRecordArray exception
                .isInstanceOf(AerospikeException.BatchRecordArray.class)
                .hasMessageContaining("Errors during batch update");
            assertThat(first.getVersion() == 2).isTrue(); // This document's version gets updated after it is read
            // from the corresponding DB record
            assertThat(second.getVersion() == 0).isTrue(); // This document's version stays equal to zero as there is
            // no corresponding DB record

            assertThat(template.findById(first.getId(), VersionedClass.class)).isEqualTo(first);
            assertThat(template.findById(second.getId(), VersionedClass.class)).isNull();

        }
    }

    @Test
    public void updateAllIfDocumentsNotChanged() {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            int age1 = 140335200;
            int age2 = 177652800;
            Person person1 = new Person(id, "Wolfgang M", age1);
            Person person2 = new Person(nextId(), "Johann B", age2);
            template.insertAll(List.of(person1, person2));
            template.updateAll(List.of(person1, person2));

            Person result1 = template.findById(person1.getId(), Person.class);
            Person result2 = template.findById(person2.getId(), Person.class);
            assertThat(result1.getAge()).isEqualTo(age1);
            assertThat(result2.getAge()).isEqualTo(age2);
            template.delete(result1); // cleanup
            template.delete(result2); // cleanup
        }
    }

    @Test
    public void updateAllIfDocumentsChanged() {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            int age1 = 140335200;
            int age2 = 177652800;
            Person person1 = new Person(id, "Wolfgang", age1);
            Person person2 = new Person(nextId(), "Johann", age2);
            template.insertAll(List.of(person1, person2));

            person1.setFirstName("Wolfgang M");
            person2.setFirstName("Johann B");
            template.updateAll(List.of(person1, person2));

            Person result1 = template.findById(person1.getId(), Person.class);
            Person result2 = template.findById(person2.getId(), Person.class);
            assertThat(result1.getAge()).isEqualTo(age1);
            assertThat(result1.getFirstName()).isEqualTo("Wolfgang M");
            assertThat(result2.getAge()).isEqualTo(age2);
            assertThat(result2.getFirstName()).isEqualTo("Johann B");
            template.delete(result1); // cleanup
            template.delete(result2); // cleanup

            List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
            Iterable<Person> personsWithUpdate = persons.stream()
                .peek(person -> person.setFirstName(person.getFirstName() + "_")).toList();
            template.updateAll(personsWithUpdate);
            personsWithUpdate.forEach(person ->
                assertThat(template.findById(person.getId(), Person.class).getFirstName()
                    .equals(person.getFirstName())).isTrue());
        }
    }

    @Test
    public void updateAllIfDocumentsNotChangedWithSetName() {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            int age1 = 140335200;
            int age2 = 177652800;
            Person person1 = new Person(id, "Wolfgang", age1);
            Person person2 = new Person(nextId(), "Johann", age2);
            template.insertAll(List.of(person1, person2), OVERRIDE_SET_NAME);
            template.updateAll(List.of(person1, person2), OVERRIDE_SET_NAME);

            Person result1 = template.findById(person1.getId(), Person.class, OVERRIDE_SET_NAME);
            Person result2 = template.findById(person2.getId(), Person.class, OVERRIDE_SET_NAME);
            assertThat(result1.getAge()).isEqualTo(age1);
            assertThat(result2.getAge()).isEqualTo(age2);
            template.delete(result1, OVERRIDE_SET_NAME); // cleanup
            template.delete(result2, OVERRIDE_SET_NAME); // cleanup
        }
    }
}
