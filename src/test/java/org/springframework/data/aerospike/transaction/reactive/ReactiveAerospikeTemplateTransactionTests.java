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
package org.springframework.data.aerospike.transaction.reactive;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeTemplateTransactionTests extends BaseReactiveIntegrationTests {

    @Autowired
    AerospikeReactiveTransactionManager transactionManager;

    @Autowired
    TransactionalOperator transactionalOperator;

    AerospikeReactiveTransactionManager mockTxManager = mock(AerospikeReactiveTransactionManager.class);
    TransactionalOperator mockTxOperator =
        TransactionalOperator.create(mockTxManager, new DefaultTransactionDefinition());


    @BeforeEach
    public void beforeEach() {
        deleteAll(Person.class, SampleClasses.DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    @AfterEach
    void verifyTransactionResourcesReleased() {
        assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty()).isTrue();
        assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();
    }

    @AfterAll
    public void afterAll() {
        deleteAll(Person.class, SampleClasses.DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    @Test
    public void insertInTransaction_verifyCommit() {
        mockTxOperator
            .transactional(reactiveTemplate.insert(new SampleClasses.DocumentWithPrimitiveIntId(100)))
            .as(StepVerifier::create)
            .verifyComplete();

        // verify that commit() was called
        verify(mockTxManager).commit(null);

        // resource holder must be already released
        assertThat(TransactionSynchronizationManager.getResource(reactiveClient)).isNull();
    }

    @Test
    public void insertInTransaction_oneWrite() {
        // Multi-record transactions are supported starting with Server version 8.0+
        var document = new SampleClasses.DocumentWithIntegerId(100, "test1");

        transactionalOperator
            .transactional(reactiveTemplate.insert(document))
            .as(StepVerifier::create)
            .expectNext(document)
            .verifyComplete();

        reactiveTemplate
            .findById(100, SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result.getContent().equals("test1")).isTrue())
            .verifyComplete();
    }
}
