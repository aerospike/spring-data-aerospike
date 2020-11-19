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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.repository.query.Criteria;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.repository.query.parser.Part;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AerospikeTemplateCountTests extends BaseBlockingIntegrationTests {

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        additionalAerospikeTestOperations.deleteAll(Person.class);
    }

    @Test
    public void countFindsAllItemsByGivenCriteria() {
        template.insert(new Person(id, "vasili", 50));
        template.insert(new Person(nextId(), "vasili", 51));
        template.insert(new Person(nextId(), "vasili", 52));
        template.insert(new Person(nextId(), "petya", 52));

        long vasyaCount = template.count(new Query(new Criteria().is("vasili", "firstName")), Person.class);

        assertThat(vasyaCount).isEqualTo(3);

        long vasya51Count = template.count(new Query(new Criteria().is("vasili", "firstName").and("age").is(51, "age")), Person.class);

        assertThat(vasya51Count).isEqualTo(1);

        long petyaCount = template.count(new Query(new Criteria().is("petya", "firstName")), Person.class);

        assertThat(petyaCount).isEqualTo(1);
    }

    @Test
    public void countFindsAllItemsByGivenCriteriaAndRespectsIgnoreCase() {
        template.insert(new Person(id, "VaSili", 50));
        template.insert(new Person(nextId(), "vasILI", 51));
        template.insert(new Person(nextId(), "vasili", 52));

        Query query1 = new Query(new Criteria().startingWith("vas", "firstName", Part.IgnoreCaseType.ALWAYS));
        assertThat(template.count(query1, Person.class)).isEqualTo(3);

        Query query2 = new Query(new Criteria().startingWith("VaS", "firstName", Part.IgnoreCaseType.NEVER));
        assertThat(template.count(query2, Person.class)).isEqualTo(1);
    }

    @Test
    public void countReturnsZeroIfNoDocumentsByProvidedCriteriaIsFound() {
        long count = template.count(new Query(new Criteria().is("nastyushka", "firstName")), Person.class);

        assertThat(count).isZero();
    }

    @Test
    public void countRejectsNullEntityClass() {
        assertThatThrownBy(() -> template.count(null, (Class<?>) null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Type must not be null!");
    }

    @Test
    void countForNotExistingSetIsZero() {
        long count = template.count(Person.class, "not-existing-set-name");

        assertThat(count).isZero();
    }

    @Test
    void countForObjects() {
        template.insert(new Person(id, "vasili", 50));
        template.insert(new Person(nextId(), "vasili", 51));
        template.insert(new Person(nextId(), "vasili", 52));
        template.insert(new Person(nextId(), "petya", 52));

        long count = template.count(Person.class);

        assertThat(count).isEqualTo(4);
    }
}
