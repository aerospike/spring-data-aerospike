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

import com.aerospike.client.policy.GenerationPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.CustomCollectionClassToDelete;
import org.springframework.data.aerospike.SampleClasses.DocumentWithExpiration;
import org.springframework.data.aerospike.SampleClasses.VersionedClass;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.utility.IndexUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.TEN_SECONDS;

public class AerospikeTemplateDeleteTests extends BaseBlockingIntegrationTests {

    @Test
    public void deleteByObject_ignoresDocumentVersionEvenIfDefaultGenerationPolicyIsSet() {
        GenerationPolicy initialGenerationPolicy = client.getWritePolicyDefault().generationPolicy;
        client.getWritePolicyDefault().generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        try {
            VersionedClass initialDocument = new VersionedClass(id, "a");
            template.insert(initialDocument);
            template.update(new VersionedClass(id, "b", initialDocument.version));

            boolean deleted = template.delete(initialDocument);
            assertThat(deleted).isTrue();
        } finally {
            client.getWritePolicyDefault().generationPolicy = initialGenerationPolicy;
        }
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

        boolean deleted = template.delete(document);
        assertThat(deleted).isTrue();

        Person result = template.findById(id, Person.class);
        assertThat(result).isNull();
    }

    @Test
    public void deleteById_deletesDocument() {
        Person document = new Person(id, "QLastName", 21);
        template.insert(document);

        boolean deleted = template.delete(id, Person.class);
        assertThat(deleted).isTrue();

        Person result = template.findById(id, Person.class);
        assertThat(result).isNull();
    }

    @Test
    public void deleteById_returnsFalseIfValueIsAbsent() {
        assertThat(template.delete(id, Person.class)).isFalse();
    }

    @Test
    public void deleteByGroupedKeys() {
        if (IndexUtils.isBatchWriteSupported(client)) {
            List<Person> persons = additionalAerospikeTestOperations.generatePersons(5);
            List<String> personsIds = persons.stream().map(Person::getId).toList();
            List<Customer> customers = additionalAerospikeTestOperations.generateCustomers(3);
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

        template.delete(CustomCollectionClassToDelete.class);

        // truncate is async operation that is why we need to wait until
        // it completes
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

        template.delete(DocumentWithExpiration.class);

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
        assertThatThrownBy(() -> template.delete(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Class must not be null!");
    }
}
