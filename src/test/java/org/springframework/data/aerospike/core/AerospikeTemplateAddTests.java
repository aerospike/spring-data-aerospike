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
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.query.cache.IndexRefresher.INDEX_CACHE_REFRESH_SECONDS;

@TestPropertySource(properties = {INDEX_CACHE_REFRESH_SECONDS + " = 0", "createIndexesOnStartup = false"})
// this test class does not require secondary indexes created on startup
public class AerospikeTemplateAddTests extends BaseBlockingIntegrationTests {

    @Test
    public void add_incrementsOneValue() {
        Person one = Person.builder().id(id).age(25).build();
        template.insert(one);

        Person updated = template.add(one, "age", 1);

        assertThat(updated.getAge()).isEqualTo(26);
        Person result = template.findById(id, Person.class);
        assertThat(result).isEqualTo(updated);
        template.delete(result);
    }

    @Test
    public void add_incrementsMultipleValues() {
        Person person = Person.builder().id(id).age(45).waist(90).build();
        template.insert(person);

        Map<String, Long> values = new HashMap<>();
        values.put("age", 10L);
        values.put("waist", 4L);
        Person updated = template.add(person, values);

        assertThat(updated.getAge()).isEqualTo(55);
        assertThat(updated.getWaist()).isEqualTo(94);
        Person result = template.findById(id, Person.class);
        assertThat(result).isEqualTo(updated);
        template.delete(result);
    }

    @Test
    public void add_incrementWithSetName() {
        Person one = Person.builder().id(id).age(25).build();
        template.insert(one, OVERRIDE_SET_NAME);

        Person updated = template.add(one, OVERRIDE_SET_NAME, "age", 1);

        assertThat(updated.getAge()).isEqualTo(26);
        Person result = template.findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(result).isEqualTo(updated);
        template.delete(result, OVERRIDE_SET_NAME);
    }

    @Test
    public void add_incrementsMultipleValuesWithSetName() {
        Person person = Person.builder().id(id).age(45).waist(90).build();
        template.insert(person, OVERRIDE_SET_NAME);

        Map<String, Long> values = new HashMap<>();
        values.put("age", 10L);
        values.put("waist", 4L);
        Person updated = template.add(person, OVERRIDE_SET_NAME, values);

        assertThat(updated.getAge()).isEqualTo(55);
        assertThat(updated.getWaist()).isEqualTo(94);
        Person result = template.findById(id, Person.class, OVERRIDE_SET_NAME);
        assertThat(result).isEqualTo(updated);
        template.delete(result, OVERRIDE_SET_NAME);
    }
}
