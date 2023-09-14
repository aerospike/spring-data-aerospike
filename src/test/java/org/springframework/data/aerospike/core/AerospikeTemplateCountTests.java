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

import com.aerospike.client.Value;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.AerospikeCriteria;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.sample.Person;

import java.time.Duration;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateCountTests extends BaseBlockingIntegrationTests {

    @BeforeAll
    public void beforeAll() {
        template.refreshIndexesCache();
    }

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
    }

    @Test
    public void countFindsAllItemsByGivenCriteria() {
        template.insert(new Person(id, "vasili", 50));
        String id2 = nextId();
        template.insert(new Person(id2, "vasili", 51));
        String id3 = nextId();
        template.insert(new Person(id3, "vasili", 52));
        String id4 = nextId();
        template.insert(new Person(id4, "petya", 52));

        long vasyaCount = template.count
            (new Query
                    (new AerospikeCriteria
                        (new Qualifier.QualifierBuilder()
                            .setFilterOperation(FilterOperation.EQ)
                            .setField("firstName")
                            .setValue1(Value.get("vasili"))
                        )
                    ),
                Person.class
            );

        assertThat(vasyaCount).isEqualTo(3);

        Qualifier.QualifierBuilder qbIs1 = new Qualifier.QualifierBuilder()
            .setFilterOperation(FilterOperation.EQ)
            .setField("firstName")
            .setValue1(Value.get("vasili"));

        Qualifier.QualifierBuilder qbIs2 = new Qualifier.QualifierBuilder()
            .setFilterOperation(FilterOperation.EQ)
            .setField("age")
            .setValue1(Value.get(51));

        long vasya51Count = template.count(new Query(new AerospikeCriteria(new Qualifier.QualifierBuilder()
                .setFilterOperation(FilterOperation.AND)
                .setQualifiers(qbIs1.build(), qbIs2.build())
            )
            ),
            Person.class
        );

        assertThat(vasya51Count).isEqualTo(1);

        long petyaCount = template.count
            (new Query
                    (new AerospikeCriteria
                        (new Qualifier.QualifierBuilder()
                            .setFilterOperation(FilterOperation.EQ)
                            .setField("firstName")
                            .setValue1(Value.get("petya"))
                        )
                    ),
                Person.class
            );

        assertThat(petyaCount).isEqualTo(1);

        template.delete(template.findById(id, Person.class));
        template.delete(template.findById(id2, Person.class));
        template.delete(template.findById(id3, Person.class));
        template.delete(template.findById(id4, Person.class));
    }

    @Test
    public void countFindsAllItemsByGivenCriteriaAndRespectsIgnoreCase() {
        template.insert(new Person(id, "VaSili", 50));
        String id2 = nextId();
        template.insert(new Person(id2, "vasILI", 51));
        String id3 = nextId();
        template.insert(new Person(id3, "vasili", 52));

        Query query1 = new Query
            (new AerospikeCriteria
                (new Qualifier.QualifierBuilder()
                    .setField("firstName")
                    .setValue1(Value.get("vas"))
                    .setFilterOperation(FilterOperation.STARTS_WITH)
                    .setIgnoreCase(true)
                )
            );
        assertThat(template.count(query1, Person.class)).isEqualTo(3);

        Query query2 = new Query
            (new AerospikeCriteria
                (new Qualifier.QualifierBuilder()
                    .setField("firstName")
                    .setValue1(Value.get("VaS"))
                    .setFilterOperation(FilterOperation.STARTS_WITH)
                    .setIgnoreCase(false)
                )
            );

        assertThat(template.count(query2, Person.class)).isEqualTo(1);

        template.delete(template.findById(id, Person.class));
        template.delete(template.findById(id2, Person.class));
        template.delete(template.findById(id3, Person.class));
    }

    @Test
    public void countReturnsZeroIfNoDocumentsByProvidedCriteriaIsFound() {
        Query query1 = new Query
            (new AerospikeCriteria
                (new Qualifier.QualifierBuilder()
                    .setField("firstName")
                    .setValue1(Value.get("nastyushka"))
                    .setFilterOperation(FilterOperation.STARTS_WITH)
                )
            );

        long count = template.count(query1, Person.class);

        assertThat(count).isZero();
    }

    @Test
    public void countRejectsNullEntityClass() {
        assertThatThrownBy(() -> template.count(null, (Class<?>) null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Class must not be null!");
    }

    @Test
    void countForNotExistingSetIsZero() {
        long count = template.count("not-existing-set-name");

        assertThat(count).isZero();
    }

    @Test
    void countForObjects() {
        template.insert(new Person(id, "vasili", 50));
        String id2 = nextId();
        template.insert(new Person(id2, "vasili", 51));
        String id3 = nextId();
        template.insert(new Person(id3, "vasili", 52));
        String id4 = nextId();
        template.insert(new Person(id4, "petya", 52));

        Awaitility.await()
            .atMost(Duration.ofSeconds(15))
            .until(() -> isCountExactlyNum(4L));

        template.delete(template.findById(id, Person.class));
        template.delete(template.findById(id2, Person.class));
        template.delete(template.findById(id3, Person.class));
        template.delete(template.findById(id4, Person.class));
    }

    @SuppressWarnings("SameParameterValue")
    private boolean isCountExactlyNum(Long num) {
        return Objects.equals(template.count(Person.class), num);
    }
}
