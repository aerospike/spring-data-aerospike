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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithByteArray;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.AsyncUtils;
import org.springframework.data.aerospike.utility.ServerVersionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;

public class AerospikeTemplateInsertTests extends BaseBlockingIntegrationTests {

    @Test
    public void insertsAndFindsWithCustomCollectionSet() {
        CustomCollectionClass initial = new CustomCollectionClass(id, "data0");
        template.insert(initial);

        Record record = client.get(new Policy(), new Key(getNameSpace(), "custom-set", id));

        assertThat(record.getString("data")).isEqualTo("data0");
        CustomCollectionClass result = template.findById(id, CustomCollectionClass.class);
        assertThat(result).isEqualTo(initial);
        template.delete(result); // cleanup
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
            .stringMap(Collections.singletonMap("k", "v"))
            .strings(Arrays.asList("a", "b", "c"))
            .friend(new Person(null, "Anna", 43))
            .isActive(true)
            .sex(Person.Sex.MALE)
            .dateOfBirth(new Date())
            .build();
        template.insert(customer);

        Person actual = template.findById(id, Person.class);
        assertThat(actual).isEqualTo(customer);
        template.delete(actual); // cleanup
    }

    @Test
    public void insertsDocumentWithListMapDateStringLongValuesAndSetName() {
        Person customer = Person.builder()
            .id(id)
            .firstName("Dave")
            .lastName("Grohl")
            .age(45)
            .waist(90)
            .emailAddress("dave@gmail.com")
            .stringMap(Collections.singletonMap("k", "v"))
            .strings(Arrays.asList("a", "b", "c"))
            .friend(new Person(null, "Anna", 43))
            .isActive(true)
            .sex(Person.Sex.MALE)
            .dateOfBirth(new Date())
            .build();
        template.insert(customer, OVERRIDE_SET_NAME);

        Person actual = template.findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(actual).isEqualTo(customer);
        template.delete(actual, OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void insertsAndFindsDocumentWithByteArrayField() {
        DocumentWithByteArray document = new DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});
        template.insert(document);

        DocumentWithByteArray result = template.findById(id, DocumentWithByteArray.class);
        assertThat(result).isEqualTo(document);
        template.delete(result); // cleanup
    }

    @Test
    public void insertsDocumentWithNullFields() {
        VersionedClass document = new VersionedClass(id, null);
        template.insert(document);

        assertThat(document.getField()).isNull();
        template.delete(template.findById(id, VersionedClass.class)); // cleanup
    }

    @Test
    public void insertsDocumentWithZeroVersionIfThereIsNoDocumentWithSameKey() {
        VersionedClass document = new VersionedClass(id, "any");
        template.insert(document);

        assertThat(document.getVersion()).isEqualTo(1);
        template.delete(template.findById(id, VersionedClass.class)); // cleanup
    }

    @Test
    public void insertsDocumentWithVersionGreaterThanZeroIfThereIsNoDocumentWithSameKey() {
        VersionedClass document = new VersionedClass(id, "any", 5L);
        // initially given versions are ignored
        // RecordExistsAction.CREATE_ONLY is set
        template.insert(document);

        assertThat(document.getVersion()).isEqualTo(1);
        template.delete(template.findById(id, VersionedClass.class)); // cleanup
    }

    @Test
    public void throwsExceptionForDuplicateId() {
        Person person = new Person(id, "Amol", 28);
        template.insert(person);

        assertThatThrownBy(() -> template.insert(person))
            .isInstanceOf(DuplicateKeyException.class);
        template.delete(template.findById(id, Person.class)); // cleanup
    }

    @Test
    public void throwsExceptionForDuplicateIdForVersionedDocument() {
        VersionedClass document = new VersionedClass(id, "any", 5L);

        template.insert(document);
        assertThatThrownBy(() -> template.insert(document))
            .isInstanceOf(DuplicateKeyException.class);
        template.delete(template.findById(id, VersionedClass.class)); // cleanup
    }

    @Test
    public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForVersionedDocument() {
        AtomicLong counter = new AtomicLong();
        AtomicLong duplicateKeyCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            try {
                template.insert(new VersionedClass(id, data));
            } catch (DuplicateKeyException e) {
                duplicateKeyCounter.incrementAndGet();
            }
        });

        assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
        template.delete(template.findById(id, VersionedClass.class)); // cleanup
    }

    @Test
    public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForNonVersionedDocument() {
        AtomicLong counter = new AtomicLong();
        AtomicLong duplicateKeyCounter = new AtomicLong();
        int numberOfConcurrentSaves = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentSaves, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            try {
                template.insert(new Person(id, data, 28));
            } catch (DuplicateKeyException e) {
                duplicateKeyCounter.incrementAndGet();
            }
        });

        assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
        template.delete(template.findById(id, Person.class)); // cleanup
    }

    @Test
    public void insertAll_insertsAllDocuments() {
        List<Person> persons = IntStream.range(1, 10)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());

        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            template.insertAll(persons);
        } else {
            persons.forEach(person -> template.insert(person));
        }

        List<Person> result = template.findByIds(persons.stream().map(Person::getId)
            .collect(Collectors.toList()), Person.class);

        assertThat(result).hasSameElementsAs(persons);
        for (Person person : result) {
            template.delete(person); // cleanup
        }
    }

    @Test
    public void insertAllWithSetName_insertsAllDocuments() {
        List<Person> persons = IntStream.range(1, 10)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());

        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            template.insertAll(persons, OVERRIDE_SET_NAME);
        } else {
            persons.forEach(person -> template.insert(person, OVERRIDE_SET_NAME));
        }

        List<Person> result = template.findByIds(persons.stream().map(Person::getId)
            .collect(Collectors.toList()), Person.class, OVERRIDE_SET_NAME);

        assertThat(result).hasSameElementsAs(persons);
        for (Person person : result) {
            template.delete(person, OVERRIDE_SET_NAME); // cleanup
        }
    }

    @Test
    public void insertAll_rejectsDuplicateIds() {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            VersionedClass first = new VersionedClass(id, "foo");

            assertThatThrownBy(() -> template.insertAll(List.of(first, first)))
                .isInstanceOf(AerospikeException.BatchRecordArray.class)
                .hasMessageContaining("Errors during batch insert");
            Assertions.assertEquals(1, (long) first.getVersion());
            template.delete(first); // cleanup
        }
    }

    @Test
    public void shouldInsertAllVersionedDocuments() {
        // batch write operations are supported starting with Server version 6.0+
        if (ServerVersionUtils.isBatchWriteSupported(client)) {
            VersionedClass first = new VersionedClass(id, "foo");
            VersionedClass second = new VersionedClass(nextId(), "foo", 1L);
            VersionedClass third = new VersionedClass(nextId(), "foo", 2L);
            template.insertAll(List.of(first));

            // initially given versions are ignored
            // RecordExistsAction.CREATE_ONLY is set
            assertThatNoException().isThrownBy(() -> template.insertAll(
                List.of(second, third)));

            assertThat(first.getVersion() == 1).isTrue();
            assertThat(second.getVersion() == 1).isTrue();
            assertThat(third.getVersion() == 1).isTrue();
        }
    }
}
