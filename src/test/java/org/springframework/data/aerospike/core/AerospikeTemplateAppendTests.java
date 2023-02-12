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

import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTemplateAppendTests extends BaseBlockingIntegrationTests {

    @Test
    public void shouldAppend() {
        Person one = Person.builder().id(id).firstName("Nas").build();
        template.insert(one);

        Person appended = template.append(one, "firstName", "tya");

        assertThat(appended).isEqualTo(Person.builder().id(id).firstName("Nastya").build());
        assertThat(appended.getFirstName()).isEqualTo("Nastya");
        assertThat(template.findById(id, Person.class).getFirstName()).isEqualTo("Nastya");
    }

    @Test
    public void shouldAppendMultipleFields() {
        Person one = Person.builder().id(id).firstName("Nas").emailAddress("nastya@").build();
        template.insert(one);

        Map<String, String> toBeUpdated = new HashMap<>();
        toBeUpdated.put("firstName", "tya");
        toBeUpdated.put("email", "gmail.com");
        Person appended = template.append(one, toBeUpdated);

        assertThat(appended.getFirstName()).isEqualTo("Nastya");
        assertThat(appended.getEmailAddress()).isEqualTo("nastya@gmail.com");
        Person actual = template.findById(id, Person.class);
        assertThat(actual.getFirstName()).isEqualTo("Nastya");
        assertThat(actual.getEmailAddress()).isEqualTo("nastya@gmail.com");
    }
}
