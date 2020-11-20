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
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.AsyncUtils;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.SampleClasses.VersionedClass;

public class AerospikeTemplateUpdateTests extends BaseBlockingIntegrationTests {

    @Test
    public void shouldThrowExceptionOnUpdateForNonexistingKey() {
        assertThatThrownBy(() -> template.update(new Person(id, "svenfirstName", 11)))
                .isInstanceOf(DataRetrievalFailureException.class);
    }

    @Test
    public void updatesEvenIfDocumentNotChanged() {
        Person person = new Person(id, "Wolfgan", 11);
        template.insert(person);

        template.update(person);

        Person result = template.findById(id, Person.class);

        assertThat(result.getAge()).isEqualTo(11);
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
    }

    @Test
    public void updatesFieldValueAndDocumentVersion() {
        VersionedClass document = new VersionedClass(id, "foobar");
        template.insert(document);
        assertThat(template.findById(id, VersionedClass.class).version).isEqualTo(1);

        document = new VersionedClass(id, "foobar1", document.version);
        template.update(document);
        assertThat(template.findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.field).isEqualTo("foobar1");
            assertThat(doc.version).isEqualTo(2);
        });

        document = new VersionedClass(id, "foobar2", document.version);
        template.update(document);
        assertThat(template.findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.field).isEqualTo("foobar2");
            assertThat(doc.version).isEqualTo(3);
        });
    }

    @Test
    public void updatesFieldToNull() {
        VersionedClass document = new VersionedClass(id, "foobar");
        template.insert(document);

        document = new VersionedClass(id, null, document.version);
        template.update(document);
        assertThat(template.findById(id, VersionedClass.class)).satisfies(doc -> {
            assertThat(doc.field).isNull();
            assertThat(doc.version).isEqualTo(2);
        });
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
        assertThat(actual.version).isEqualTo(3);
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
                template.update(new VersionedClass(id, data, document.version));
            } catch (OptimisticLockingFailureException e) {
                optimisticLock.incrementAndGet();
            }
        });

        assertThat(optimisticLock.intValue()).isEqualTo(numberOfConcurrentSaves - 1);
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
                .map(map)
                .strings(list)
                .build();

        template.insert(person);

        Person personWithList = template.findById(id, Person.class);
        personWithList.getStrings().add("Added something new");
        template.update(personWithList);

        Person personWithList2 = template.findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getStrings()).hasSize(4);
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
                .map(map)
                .strings(list)
                .build();
        template.insert(person);

        Person personWithList = template.findById(id, Person.class);
        personWithList.getMap().put("key4", "Added something new");
        template.update(personWithList);

        Person personWithList2 = template.findById(id, Person.class);
        assertThat(personWithList2).isEqualTo(personWithList);
        assertThat(personWithList2.getMap()).hasSize(4);
        assertThat(personWithList2.getMap().get("key4")).isEqualTo("Added something new");
    }
}
