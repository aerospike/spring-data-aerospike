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
package org.springframework.data.aerospike.core.sync;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import lombok.Data;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.core.convert.ConversionFailedException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses.CustomCollectionClass;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithByteArray;
import org.springframework.data.aerospike.util.AsyncUtils;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;

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

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateInsertTests extends BaseBlockingIntegrationTests {

    @BeforeEach
    public void beforeEach() {
        deleteAll(Person.class, CustomCollectionClass.class, DocumentWithByteArray.class, VersionedClass.class,
            OVERRIDE_SET_NAME);
    }

    @AfterAll
    public void afterAll() {
        deleteAll(Person.class, CustomCollectionClass.class, DocumentWithByteArray.class, VersionedClass.class,
            OVERRIDE_SET_NAME);
    }

    @Test
    public void insertsAndFindsWithCustomCollectionSet() {
        CustomCollectionClass initial = new CustomCollectionClass(id, "data0");
        template.insert(initial);

        Record record = client.get(new Policy(), new Key(getNameSpace(), "custom-set", id));

        assertThat(record.getString("data")).isEqualTo("data0");
        CustomCollectionClass result = template.findById(id, CustomCollectionClass.class);
        assertThat(result).isEqualTo(initial);
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
            .gender(Person.Gender.MALE)
            .dateOfBirth(new Date())
            .build();
        template.insert(customer);

        Person actual = template.findById(id, Person.class);
        assertThat(actual).isEqualTo(customer);
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
            .gender(Person.Gender.MALE)
            .dateOfBirth(new Date())
            .build();
        template.insert(customer, OVERRIDE_SET_NAME);

        Person actual = template.findById(id, Person.class, OVERRIDE_SET_NAME);
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
        VersionedClass document = new VersionedClass(id, "any", 5);
        // initially given versions are ignored
        // RecordExistsAction.CREATE_ONLY is used
        template.insert(document);

        assertThat(document.getVersion()).isEqualTo(1);
    }


    @Data
    @Document(collection = "versioned-set")
    public static class ClassWithLongVersion {

        @Version
        private long version; // must be integer
        private String field;
        @Id
        private String id;

        public ClassWithLongVersion(String id, String field, long version) {
            this.id = id;
            this.field = field;
            this.version = version;
        }
    }

    @Test
    public void mustUseIntegerVersion() {
        ClassWithLongVersion document = new ClassWithLongVersion(id, "any", Long.MAX_VALUE);
        assertThatThrownBy(() -> template.insert(document))
            .isInstanceOf(ConversionFailedException.class)
            .hasMessageContaining("Failed to convert from type [java.lang.Long] to type [java.lang.Integer]");
    }

    @Test
    public void throwsExceptionForDuplicateId() {
        Person person = new Person(id, "Amol", 28);
        template.insert(person);

        assertThatThrownBy(() -> template.insert(person))
            .isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    public void throwsExceptionForBatchInsertingDuplicateId() {
        assertThatThrownBy(() -> template.insertAll(
            List.of(
                new Person(id, "svenfirstName", 11),
                new Person(id, "svenfirstName", 11)
                )
        ))
            .isInstanceOf(AerospikeException.BatchRecordArray.class);
    }

    @Test
    public void throwsExceptionForDuplicateIdForVersionedDocument() {
        VersionedClass document = new VersionedClass(id, "any", 5);

        template.insert(document);
        assertThatThrownBy(() -> template.insert(document))
            .isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForVersionedDocument() {
        AtomicLong counter = new AtomicLong();
        AtomicLong duplicateKeyCounter = new AtomicLong();
        int numberOfConcurrentInserts = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentInserts, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            try {
                template.insert(new VersionedClass(id, data));
            } catch (DuplicateKeyException e) {
                duplicateKeyCounter.incrementAndGet();
            }
        });

        assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentInserts - 1);
    }

    @Test
    public void insertsOnlyFirstDocumentAndNextAttemptsShouldFailWithDuplicateKeyExceptionForNonVersionedDocument() {
        AtomicLong counter = new AtomicLong();
        AtomicLong duplicateKeyCounter = new AtomicLong();
        int numberOfConcurrentInserts = 5;

        AsyncUtils.executeConcurrently(numberOfConcurrentInserts, () -> {
            long counterValue = counter.incrementAndGet();
            String data = "value-" + counterValue;
            try {
                template.insert(new Person(id, data, 28));
            } catch (DuplicateKeyException e) {
                duplicateKeyCounter.incrementAndGet();
            }
        });

        assertThat(duplicateKeyCounter.intValue()).isEqualTo(numberOfConcurrentInserts - 1);
    }

    @Test
    public void insertAll_insertsAllDocuments() {
        List<Person> persons = IntStream.range(1, 10)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());
        template.insertAll(persons);

        List<Person> result = template.findByIds(persons.stream().map(Person::getId)
            .collect(Collectors.toList()), Person.class);
        assertThat(result).hasSameElementsAs(persons);
        template.deleteAll(Person.class); // cleanup

        Iterable<Person> personsToInsert = IntStream.range(0, 101)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());
        template.insertAll(personsToInsert);

        @SuppressWarnings("CastCanBeRemovedNarrowingVariableType")
        List<String> ids = ((List<Person>) personsToInsert).stream().map(Person::getId)
            .collect(Collectors.toList());
        result = template.findByIds(ids, Person.class);
        assertThat(result).hasSameElementsAs(personsToInsert);
    }

    @Test
    public void insertAllWithSetName_insertsAllDocuments() {
        List<Person> persons = IntStream.range(1, 10)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());

        template.insertAll(persons, OVERRIDE_SET_NAME);

        List<Person> result = template.findByIds(persons.stream().map(Person::getId)
            .collect(Collectors.toList()), Person.class, OVERRIDE_SET_NAME);

        assertThat(result).hasSameElementsAs(persons);
    }

    @Test
    public void insertAll_rejectsDuplicateIds() {
        VersionedClass second = new VersionedClass("as-5440", "foo");
        assertThatThrownBy(() -> template.insertAll(List.of(second, second)))
            .isInstanceOf(OptimisticLockingFailureException.class)
            .hasMessageContaining("Failed to insert the record with ID 'as-5440' due to versions mismatch");
        Assertions.assertEquals(1, second.getVersion());
    }

    @Test
    public void shouldInsertAllVersionedDocuments() {
        VersionedClass first = new VersionedClass(id, "foo");
        VersionedClass second = new VersionedClass(nextId(), "foo", 1);
        VersionedClass third = new VersionedClass(nextId(), "foo", 2);

        // initially given versions are ignored
        // RecordExistsAction.CREATE_ONLY is used
        assertThatNoException().isThrownBy(() -> template.insertAll(List.of(first, second, third)));

        assertThat(first.getVersion() == 1).isTrue();
        assertThat(second.getVersion() == 1).isTrue();
        assertThat(third.getVersion() == 1).isTrue();
    }
}
