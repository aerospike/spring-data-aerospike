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

import org.awaitility.Awaitility;
import com.aerospike.client.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

public class AerospikeTemplateCountTests extends BaseBlockingIntegrationTests {

    @Override
    @BeforeEach
    public void setUp() {
        super.setUp();
        additionalAerospikeTestOperations.deleteAllAndVerify(Person.class);
    }

    @Test
    public void countFindsAllItemsByGivenCriteria() {
        template.insert(new Person(id, "vasili", 50));
        template.insert(new Person(nextId(), "vasili", 51));
        template.insert(new Person(nextId(), "vasili", 52));
        template.insert(new Person(nextId(), "petya", 52));

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
    }

    @Test
    public void countFindsAllItemsByGivenCriteriaAndRespectsIgnoreCase() {
        template.insert(new Person(id, "VaSili", 50));
        template.insert(new Person(nextId(), "vasILI", 51));
        template.insert(new Person(nextId(), "vasili", 52));

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
                .hasMessage("Type must not be null!");
    }

    @Test
    void countForNotExistingSetIsZero() {
        long count = template.count("not-existing-set-name");

        assertThat(count).isZero();
    }

    @Test
    void countForObjects() {
        template.insert(new Person(id, "vasili", 50));
        template.insert(new Person(nextId(), "vasili", 51));
        template.insert(new Person(nextId(), "vasili", 52));
        template.insert(new Person(nextId(), "petya", 52));

        Awaitility.await()
                .atMost(Duration.ofSeconds(15))
                .until(() -> isCountExactlyNum(4L));
    }

    private boolean isCountExactlyNum(Long num) {
        return Objects.equals(template.count(Person.class), num);
    }
}
