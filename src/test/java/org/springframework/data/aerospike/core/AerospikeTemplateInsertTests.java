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

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.SampleClasses.DocumentWithByteArray;
import org.springframework.data.aerospike.sample.Person;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.SampleClasses.VersionedClass;

public class AerospikeTemplateInsertTests extends BaseBlockingIntegrationTests {

    @Test
    public void insertsAndFindsWithCustomCollectionSet() {
        CustomCollectionClass initial = new CustomCollectionClass(id, "data0");
        template.insert(initial);

        Record record = client.get(new Policy(), new Key(getNameSpace(), "custom-set", id));

        assertThat(record.getString("data")).isEqualTo("data0");
        assertThat(template.findById(id, CustomCollectionClass.class)).isEqualTo(initial);
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
                .strings(Arrays.asList("a", "b", "c"))
                .friend(new Person(null, "Anna", 43))
                .active(true)
                .sex(Person.Sex.MALE)
                .dateOfBirth(new Date())
                .build();

        template.insert(customer);

        Person actual = template.findById(id, Person.class);
        assertThat(actual).isEqualTo(customer);
    }

    @Test
    public void insertsAndFindsDocumentWithByteArrayField() {
        DocumentWithByteArray document = new DocumentWithByteArray(id, new byte[]{1, 0, 0, 1, 1, 1, 0, 0});

        template.insert(document);

        DocumentWithByteArray result = template.findById(id, DocumentWithByteArray.class);
        assertThat(result).isEqualTo(document);
    }

    @Test
    public void insertsDocumentWithNullFields() {
        VersionedClass document = new VersionedClass(id, null);

        template.insert(document);

        assertThat(document.getField()).isNull();
    }

    @Test
    public void insertsDocumentWithZeroVersionIfThereIsNoDocumentWithSameKey() {
        VersionedClass document = new VersionedClass(id, "any");

        template.insert(document);

        assertThat(document.getVersion()).isEqualTo(1);
    }

    @Test
    public void insertsDocumentWithVersionGreaterThanZeroIfThereIsNoDocumentWithSameKey() {
        VersionedClass document = new VersionedClass(id, "any", 5L);

        template.insert(document);

        assertThat(document.getVersion()).isEqualTo(1);
    }

    @Test
    public void throwsExceptionForDuplicateId() {
        Person person = new Person(id, "Amol", 28);

        template.insert(person);
        assertThatThrownBy(() -> template.insert(person))
                .isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    public void throwsExceptionForDuplicateIdForVersionedDocument() {
        VersionedClass document = new VersionedClass(id, "any", 5L);

        template.insert(document);
        assertThatThrownBy(() -> template.insert(document))
                .isInstanceOf(DuplicateKeyException.class);
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

    }

    @Test
    public void insertAll_rejectsDuplicateIds() {
        Person person = Person.builder().id(id).build();
        List<Person> records = Arrays.asList(person, person);

        assertThatThrownBy(() -> template.insertAll(records))
                .isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    public void insertAll_insertsAllDocuments() {
        List<Person> persons = IntStream.range(1, 10)
                .mapToObj(age -> Person.builder().id(nextId())
                        .firstName("Gregor")
                        .age(age).build())
                .collect(Collectors.toList());
        template.insertAll(persons);

        List<Person> result = template.findByIds(persons.stream().map(Person::getId).collect(Collectors.toList()), Person.class);

        assertThat(result).hasSameElementsAs(persons);
    }
}
