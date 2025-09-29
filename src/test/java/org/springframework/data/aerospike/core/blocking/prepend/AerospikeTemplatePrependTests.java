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
package org.springframework.data.aerospike.core.blocking.prepend;

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTemplatePrependTests extends BaseBlockingIntegrationTests {

    @Test
    public void shouldPrepend() {
        Person one = Person.builder().id(id).firstName("tya").build();
        template.insert(one);
        Person appended = template.prepend(one, "firstName", "Nas");

        assertThat(appended.getFirstName()).isEqualTo("Nastya");
        Person result = template.findById(id, Person.class);
        assertThat(result.getFirstName()).isEqualTo("Nastya");
        template.delete(result); // cleanup
    }

    @Test
    public void shouldPrependWithSetName() {
        Person one = Person.builder().id(id).firstName("tya").build();
        template.insert(one, OVERRIDE_SET_NAME);
        Person appended = template.prepend(one, OVERRIDE_SET_NAME, "firstName", "Nas");

        assertThat(appended.getFirstName()).isEqualTo("Nastya");
        Person result = template.findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(result.getFirstName()).isEqualTo("Nastya");
        template.delete(result, OVERRIDE_SET_NAME); // cleanup
    }

    @Test
    public void shouldPrependMultipleFields() {
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        template.insert(one);

        Map<String, String> toBeUpdated = new HashMap<>();
        toBeUpdated.put("firstName", "Nas");
        toBeUpdated.put("email", "nastya@");
        Person appended = template.prepend(one, toBeUpdated);

        assertThat(appended.getFirstName()).isEqualTo("Nastya");
        assertThat(appended.getEmailAddress()).isEqualTo("nastya@gmail.com");
        Person actual = template.findById(id, Person.class);
        assertThat(actual.getFirstName()).isEqualTo("Nastya");
        assertThat(actual.getEmailAddress()).isEqualTo("nastya@gmail.com");
        template.delete(actual);
    }

    @Test
    public void shouldPrependMultipleFieldsWithSetName() {
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        template.insert(one, OVERRIDE_SET_NAME);

        Map<String, String> toBeUpdated = new HashMap<>();
        toBeUpdated.put("firstName", "Nas");
        toBeUpdated.put("email", "nastya@");
        Person appended = template.prepend(one, OVERRIDE_SET_NAME, toBeUpdated);

        assertThat(appended.getFirstName()).isEqualTo("Nastya");
        assertThat(appended.getEmailAddress()).isEqualTo("nastya@gmail.com");
        Person actual = template.findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(actual.getFirstName()).isEqualTo("Nastya");
        assertThat(actual.getEmailAddress()).isEqualTo("nastya@gmail.com");
        template.delete(actual, OVERRIDE_SET_NAME);
    }
}
