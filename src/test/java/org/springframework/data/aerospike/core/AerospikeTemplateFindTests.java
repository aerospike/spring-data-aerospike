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

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnRead;
import org.springframework.data.aerospike.SampleClasses.VersionedClassWithAllArgsConstructor;
import org.springframework.data.aerospike.sample.Person;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.SampleClasses.DocumentWithTouchOnReadAndExpirationProperty;
import static org.springframework.data.aerospike.SampleClasses.EXPIRATION_ONE_MINUTE;

public class AerospikeTemplateFindTests extends BaseBlockingIntegrationTests {

    @Test
    public void findById_shouldReadVersionedClassWithAllArgsConstructor() {
        VersionedClassWithAllArgsConstructor inserted = new VersionedClassWithAllArgsConstructor(id, "foobar", 0L);
        template.insert(inserted);

        assertThat(template.findById(id, VersionedClassWithAllArgsConstructor.class).version).isEqualTo(1L);

        template.update(new VersionedClassWithAllArgsConstructor(id, "foobar1", inserted.version));

        assertThat(template.findById(id, VersionedClassWithAllArgsConstructor.class).version).isEqualTo(2L);
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKey() {
        Person one = template.findById("person-non-existing-key", Person.class);

        assertThat(one).isNull();
    }

    @Test
    public void findById_shouldReturnNullForNonExistingKeyIfTouchOnReadSetToTrue() {
        DocumentWithTouchOnRead one = template.findById("non-existing-key", DocumentWithTouchOnRead.class);

        assertThat(one).isNull();
    }

    @Test
    public void findById_shouldIncreaseVersionIfTouchOnReadSetToTrue() {
        DocumentWithTouchOnRead doc = new DocumentWithTouchOnRead(String.valueOf(id));
        template.save(doc);

        DocumentWithTouchOnRead actual = template.findById(doc.getId(), DocumentWithTouchOnRead.class);

        assertThat(actual.getVersion()).isEqualTo(doc.getVersion() + 1);
    }

    @Test
    public void findbyIdFail() {
        Person person = new Person(id, "Oliver");
        person.setAge(25);
        template.insert(person);

        Person person1 = template.findById("Person", Person.class);
        assertThat(person1).isNull();
    }

    @Test
    public void findByIds_shouldFindExisting() {
        Person firstPerson = Person.builder().id(nextId()).firstName("first").emailAddress("gmail.com").build();
        Person secondPerson = Person.builder().id(nextId()).firstName("second").emailAddress("gmail.com").build();
        template.save(firstPerson);
        template.save(secondPerson);

        List<String> ids = Arrays.asList(nextId(), firstPerson.getId(), secondPerson.getId());

        List<Person> actual = template.findByIds(ids, Person.class);

        assertThat(actual).containsExactly(firstPerson, secondPerson);
    }


    @Test
    public void findByIds_shouldReturnEmptyList() {
        List<Person> actual = template.findByIds(Collections.emptyList(), Person.class);
        assertThat(actual).isEmpty();
    }

    @Test
    public void findById_shouldFailOnTouchOnReadWithExpirationProperty() {
        template.insert(new DocumentWithTouchOnReadAndExpirationProperty(id, EXPIRATION_ONE_MINUTE));
        assertThatThrownBy(() -> template.findById(id, DocumentWithTouchOnReadAndExpirationProperty.class))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void findByKey() {
        client.put(null, new Key(getNameSpace(), "Person", id),
                new Bin("firstName", "Dave"),
                new Bin("age", 56));

        Person result = template.findById(id, Person.class);

        assertThat(result.getFirstName()).isEqualTo("Dave");
        assertThat(result.getAge()).isEqualTo(56);
    }
}
