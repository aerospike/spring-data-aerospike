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

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeTemplateExistsTests extends BaseBlockingIntegrationTests {

    @Test
    public void exists_shouldReturnTrueIfValueIsPresent() {
        Person one = Person.builder().id(id).firstName("tya").emailAddress("gmail.com").build();
        template.insert(one);

        assertThat(template.exists(id, Person.class)).isTrue();
        template.delete(one);
    }

    @Test
    public void exists_shouldReturnFalseIfValueIsAbsent() {
        assertThat(template.exists(id, Person.class)).isFalse();
    }
}
