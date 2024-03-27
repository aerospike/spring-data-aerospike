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
import com.aerospike.client.policy.GenerationPolicy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses.CollectionOfObjects;
import org.springframework.data.aerospike.sample.SampleClasses.CustomCollectionClassToDelete;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithExpiration;
import org.springframework.data.aerospike.sample.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.util.AwaitilityUtils;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.TEN_SECONDS;

public class AerospikeTemplateDeleteTests extends BaseBlockingIntegrationTests {

    @BeforeEach
    public void beforeEach() {
        template.deleteAll(Person.class);
        template.deleteAll(Customer.class);
        template.deleteAll(VersionedClass.class);
        template.deleteAll(CollectionOfObjects.class);
    }

    @Test
    public void deleteByObject_ignoresVersionEvenIfDefaultGenerationPolicyIsSet() {
        GenerationPolicy initialGenerationPolicy = client.getWritePolicyDefault().generationPolicy;
        client.getWritePolicyDefault().generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        try {
            Person initialDocument = new Person(id, "a");
            template.insert(initialDocument);
            template.update(new Person(id, "b"));

            boolean deleted = template.delete(initialDocument);
            assertThat(deleted).isTrue();
        } finally {
            client.getWritePolicyDefault().generationPolicy = initialGenerationPolicy;
        }
    }

    @Test
    public void deleteByObject_deletesDocument() {
        Person document = new Person(id, "QLastName", 21);
        template.insert(document);
        VersionedClass versionedDocument = new VersionedClass(nextId(), "test");
        template.insert(versionedDocument);

        boolean deleted = template.delete(document);
        assertThat(deleted).isTrue();
        Person result = template.findById(id, Person.class);
        assertThat(result).isNull();

        boolean deleted2 = template.delete(versionedDocument);
        assertThat(deleted2).isTrue();
        VersionedClass result2 = template.findById(versionedDocument.getId(), VersionedClass.class);
        assertThat(result2).isNull();
    }

    @Test
    public void deleteByObject_deletesDocumentWithSetName() {
        Person person = new Person(id, "QLastName", 21);
        template.insert(person, OVERRIDE_SET_NAME);
        String id2 = nextId();
        VersionedClass versionedDocument = new VersionedClass(id2, "test");
        template.insert(versionedDocument, OVERRIDE_SET_NAME);

        boolean deleted = template.delete(person, OVERRIDE_SET_NAME);
        assertThat(deleted).isTrue();
        Person result = template.findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(result).isNull();

        boolean deleted2 = template.delete(versionedDocument, OVERRIDE_SET_NAME);
        assertThat(deleted2).isTrue();
        VersionedClass result2 = template.findById(id2, VersionedClass.class);
        assertThat(result2).isNull();
    }

    @Test
    public void deleteByObject_VersionsMismatch() {
        VersionedClass versionedDocument = new VersionedClass(nextId(), "test");

        template.insert(versionedDocument);
        versionedDocument.setVersion(2);
        assertThatThrownBy(() -> template.delete(versionedDocument))
            .isInstanceOf(OptimisticLockingFailureException.class)
            .hasMessage("Failed to delete record due to versions mismatch");
    }

    @Test
    public void deleteById_deletesDocument() {
        Person document = new Person(id, "QLastName", 21);
        template.insert(document);
        String id2 = nextId();
        VersionedClass versionedDocument = new VersionedClass(id2, "test");
        template.insert(versionedDocument);

        boolean deleted = template.deleteById(id, Person.class);
        assertThat(deleted).isTrue();
        Person result = template.findById(id, Person.class);
        assertThat(result).isNull();

        boolean deleted2 = template.deleteById(id2, VersionedClass.class);
        assertThat(deleted2).isTrue();
        VersionedClass result2 = template.findById(id2, VersionedClass.class);
        assertThat(result2).isNull();
    }

    @Test
    public void deleteById_deletesDocumentWithSetName() {
        Person document = new Person(id, "QLastName", 21);
        template.insert(document, OVERRIDE_SET_NAME);
        String id2 = nextId();
        VersionedClass versionedDocument = new VersionedClass(id2, "test");
        template.insert(versionedDocument, OVERRIDE_SET_NAME);

        boolean deleted = template.deleteById(id, OVERRIDE_SET_NAME);
        assertThat(deleted).isTrue();
        Person result = template.findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(result).isNull();

        boolean deleted2 = template.deleteById(id2, OVERRIDE_SET_NAME);
        assertThat(deleted2).isTrue();
        VersionedClass result2 = template.findById(id2, VersionedClass.class, OVERRIDE_SET_NAME);
        assertThat(result2).isNull();
    }

    @Test
    public void deleteById_returnsFalseIfValueIsAbsent() {
        assertThat(template.deleteById(id, Person.class)).isFalse();

        assertThat(template.delete(new Person(id, "QLastName", 21))).isFalse();
        assertThat(template.delete(new VersionedClass(nextId(), "test"))).isFalse();
    }

    @Test
    public void deleteByGroupedKeys() {
        if (serverVersionSupport.isBatchWriteSupported()) {
            List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(5);
            List<String> personsIds = persons.stream().map(Person::getId).toList();
            List<Customer> customers = additionalAerospikeTestOperations.saveGeneratedCustomers(3);
            List<String> customersIds = customers.stream().map(Customer::getId).toList();

            GroupedKeys groupedKeys = getGroupedKeys(persons, customers);

            template.deleteByIds(groupedKeys);

            assertThat(template.findByIds(personsIds, Person.class)).isEmpty();
            assertThat(template.findByIds(customersIds, Customer.class)).isEmpty();
        }
    }

    GroupedKeys getGroupedKeys(Collection<Person> persons, Collection<Customer> customers) {
        Set<String> requestedPersonsIds = persons.stream()
            .map(Person::getId)
            .collect(Collectors.toSet());
        Set<String> requestedCustomerIds = customers.stream().map(Customer::getId)
            .collect(Collectors.toSet());

        return GroupedKeys.builder()
            .entityKeys(Person.class, requestedPersonsIds)
            .entityKeys(Customer.class, requestedCustomerIds)
            .build();
    }

    @Test
    public void deleteByObject_returnsFalseIfValueIsAbsent() {
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        assertThat(template.delete(one)).isFalse();
    }

    @Test
    public void deleteByType_ShouldDeleteAllDocumentsWithCustomSetName() {
        String id1 = nextId();
        String id2 = nextId();
        template.save(new CustomCollectionClassToDelete(id1));
        template.save(new CustomCollectionClassToDelete(id2));

        assertThat(template.findByIds(Arrays.asList(id1, id2), CustomCollectionClassToDelete.class)).hasSize(2);

        template.deleteAll(CustomCollectionClassToDelete.class);

        // truncate is async operation that is why we need to wait until it completes
        await().atMost(TEN_SECONDS)
            .untilAsserted(() -> assertThat(template.findByIds(Arrays.asList(id1, id2),
                CustomCollectionClassToDelete.class)).isEmpty());
    }

    @Test
    public void deleteByType_ShouldDeleteAllDocumentsWithDefaultSetName() {
        String id1 = nextId();
        String id2 = nextId();
        template.save(new DocumentWithExpiration(id1));
        template.save(new DocumentWithExpiration(id2));

        template.deleteAll(DocumentWithExpiration.class);

        // truncate is async operation that is why we need to wait until
        // it completes
        await().atMost(TEN_SECONDS)
            .untilAsserted(() -> {
                assertThat(template.findById(id1, DocumentWithExpiration.class)).isNull();
                assertThat(template.findById(id2, DocumentWithExpiration.class)).isNull();
            });
    }

    @Test
    public void deleteByType_NullTypeThrowsException() {
        assertThatThrownBy(() -> template.deleteAll((Class<?>) null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Class must not be null!");
    }

    @Test
    public void deleteByIds_rejectsDuplicateIds() {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = nextId();
            DocumentWithExpiration document1 = new DocumentWithExpiration(id1);
            DocumentWithExpiration document2 = new DocumentWithExpiration(id1);
            template.save(document1);
            template.save(document2);

            List<String> ids = List.of(id1, id1);
            assertThatThrownBy(() -> template.deleteByIds(ids, DocumentWithExpiration.class))
                .isInstanceOf(AerospikeException.BatchRecordArray.class)
                .hasMessageContaining("Errors during batch delete");
        }
    }

    @Test
    public void deleteByIds_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = nextId();
            String id2 = nextId();
            template.save(new DocumentWithExpiration(id1));
            template.save(new DocumentWithExpiration(id2));

            List<String> ids = List.of(id1, id2);
            template.deleteByIds(ids, DocumentWithExpiration.class);
            assertThat(template.findByIds(ids, DocumentWithExpiration.class)).isEmpty();

            List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
            ids = persons.stream().map(Person::getId).toList();
            template.deleteByIds(ids, Person.class);
            assertThat(template.findByIds(ids, Person.class)).isEmpty();

            List<Person> persons2 = additionalAerospikeTestOperations.saveGeneratedPersons(1001);
            ids = persons2.stream().map(Person::getId).toList();
            template.deleteByIds(ids, Person.class);
            assertThat(template.findByIds(ids, Person.class)).isEmpty();
        }
    }

    @Test
    public void deleteByIds_ShouldDeleteAllDocumentsWithSetName() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = nextId();
            String id2 = nextId();
            template.save(new DocumentWithExpiration(id1), OVERRIDE_SET_NAME);
            template.save(new DocumentWithExpiration(id2), OVERRIDE_SET_NAME);

            List<String> ids = List.of(id1, id2);
            template.deleteByIds(ids, OVERRIDE_SET_NAME);

            assertThat(template.findByIds(ids, DocumentWithExpiration.class, OVERRIDE_SET_NAME)).isEmpty();
        }
    }

    @Test
    public void deleteAll_rejectsDuplicateIds() {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = nextId();
            DocumentWithExpiration document1 = new DocumentWithExpiration(id1);
            DocumentWithExpiration document2 = new DocumentWithExpiration(id1);
            template.save(document1);
            template.save(document2);

            assertThatThrownBy(() -> template.deleteAll(List.of(document1, document2)))
                .isInstanceOf(AerospikeException.BatchRecordArray.class)
                .hasMessageContaining("Errors during batch delete");
        }
    }

    @Test
    public void deleteAll_ShouldDeleteAllDocuments() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = nextId();
            String id2 = nextId();
            DocumentWithExpiration document1 = new DocumentWithExpiration(id1);
            DocumentWithExpiration document2 = new DocumentWithExpiration(id2);
            template.save(document1);
            template.save(document2);

            template.deleteAll(List.of(document1, document2));
            assertThat(template.findByIds(List.of(id1, id2), DocumentWithExpiration.class)).isEmpty();

            List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
            template.deleteAll(persons);
            List<String> personsIds = persons.stream().map(Person::getId).toList();
            assertThat(template.findByIds(personsIds, Person.class)).isEmpty();

            List<Person> persons2 = additionalAerospikeTestOperations.saveGeneratedPersons(1001);
            template.deleteAll(persons2);
            personsIds = persons2.stream().map(Person::getId).toList();
            assertThat(template.findByIds(personsIds, Person.class)).isEmpty();
        }
    }

    @Test
    public void deleteAll_ShouldDeleteAllDocumentsWithSetName() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = nextId();
            String id2 = nextId();
            DocumentWithExpiration document1 = new DocumentWithExpiration(id1);
            DocumentWithExpiration document2 = new DocumentWithExpiration(id2);
            template.saveAll(List.of(document1, document2), OVERRIDE_SET_NAME);

            template.deleteAll(List.of(document1, document2), OVERRIDE_SET_NAME);

            assertThat(template.findByIds(List.of(id1, id2), DocumentWithExpiration.class, OVERRIDE_SET_NAME)).isEmpty();
        }
    }

    @Test
    public void deleteAll_ShouldDeleteAllDocumentsBeforeGivenLastUpdateTime() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = nextId();
            String id2 = nextId();
            CollectionOfObjects document1 = new CollectionOfObjects(id1, List.of("test1"));
            CollectionOfObjects document2 = new CollectionOfObjects(id2, List.of("test2"));

            template.save(document1);
            AwaitilityUtils.wait(1, MILLISECONDS);

            Instant lastUpdateTime = Instant.now();
            Instant inFuture = Instant.ofEpochMilli(lastUpdateTime.toEpochMilli() + 10000);
            template.save(document2);

            // make sure document1 has lastUpdateTime less than specified millis
            List<CollectionOfObjects> resultsWithLutLtMillis =
                runLastUpdateTimeQuery(lastUpdateTime.toEpochMilli(), FilterOperation.LT, CollectionOfObjects.class);
            assertThat(resultsWithLutLtMillis.get(0).getId()).isEqualTo(document1.getId());
            assertThat(resultsWithLutLtMillis.get(0).getCollection().iterator().next())
                .isEqualTo(document1.getCollection().iterator().next());

            assertThatThrownBy(() -> template.deleteAll(CollectionOfObjects.class, inFuture))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("Last update time (.*) must be less than the current time");

            template.deleteAll(CollectionOfObjects.class, lastUpdateTime);
            assertThat(template.findByIds(List.of(id1, id2), CollectionOfObjects.class)).hasSize(1);
            CollectionOfObjects result = template.findByIds(List.of(id1, id2), CollectionOfObjects.class).get(0);
            assertThat(result.getId()).isEqualTo(document2.getId());
            assertThat(result.getCollection().iterator().next()).isEqualTo(document2.getCollection().iterator().next());

            List<Person> persons = additionalAerospikeTestOperations.saveGeneratedPersons(101);
            AwaitilityUtils.wait(1, MILLISECONDS);
            lastUpdateTime = Instant.now();
            AwaitilityUtils.wait(1, MILLISECONDS);
            Person newPerson = new Person(nextId(), "testFirstName");
            template.save(newPerson);
            persons.add(newPerson);

            template.deleteAll(template.getSetName(Person.class), lastUpdateTime);
            List<String> personsIds = persons.stream().map(Person::getId).toList();
            assertThat(template.findByIds(personsIds, Person.class)).contains(newPerson);

            List<Person> persons2 = additionalAerospikeTestOperations.saveGeneratedPersons(1001);
            template.deleteAll(Person.class, lastUpdateTime); // persons2 were saved after the given time
            personsIds = persons2.stream().map(Person::getId).toList();
            assertThat(template.findByIds(personsIds, Person.class)).containsExactlyElementsOf(persons2);
        }
    }

    @Test
    public void deleteAll_VersionsMismatch() {
        // batch delete operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            String id1 = "id1";
            VersionedClass document1 = new VersionedClass(id1, "test1");
            String id2 = "id2";
            VersionedClass document2 = new VersionedClass(id2, "test2");
            template.save(document1);
            template.save(document2);

            document2.setVersion(232);
            assertThatThrownBy(() -> template.deleteAll(List.of(document1, document2)))
                .isInstanceOf(OptimisticLockingFailureException.class)
                .hasMessageContaining("Failed to delete the record with ID 'id2' due to versions mismatch");
        }
    }
}
