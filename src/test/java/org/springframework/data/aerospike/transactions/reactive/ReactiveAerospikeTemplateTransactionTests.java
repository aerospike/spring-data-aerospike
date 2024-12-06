/*
 * Copyright 2024 the original author or authors
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
package org.springframework.data.aerospike.transactions.reactive;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithPrimitiveIntId;
import org.springframework.data.aerospike.util.AsyncUtils;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeTemplateTransactionTests extends BaseReactiveIntegrationTests {

    @Autowired
    AerospikeReactiveTransactionManager transactionManager;

    @Autowired
    @Qualifier("reactiveTransactionalOperator")
    TransactionalOperator transactionalOperator;

    @Autowired
    @Qualifier("reactiveTransactionalOperatorWithTimeout2")
    TransactionalOperator transactionalOperatorWithTimeout2;

    AerospikeReactiveTransactionManager mockTxManager = mock(AerospikeReactiveTransactionManager.class);
    TransactionalOperator mockTxOperator =
        TransactionalOperator.create(mockTxManager, new DefaultTransactionDefinition());

    @BeforeAll
    public void beforeAll() {
//        TestUtils.checkAssumption(serverVersionSupport.isMRTSupported(),
//            "Skipping transactions tests because Aerospike Server 8.0.0+ is required", log);
    }

    @BeforeEach
    public void beforeEach() {
        deleteAll(Person.class, DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    @AfterEach
    void verifyTransactionResourcesReleased() {
        assertThat(TransactionSynchronizationManager.getResourceMap().isEmpty()).isTrue();
        assertThat(TransactionSynchronizationManager.isSynchronizationActive()).isFalse();
    }

    @AfterAll
    public void afterAll() {
        deleteAll(Person.class, DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    @Test
    public void verifyOneWriteInTransaction() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document = new SampleClasses.DocumentWithIntegerId(500, "test1");

        // only for testing purposes as performing one write in a transaction lacks sense
        reactiveTemplate.insert(document)
            // Declaratively apply transaction boundary to the reactive operation
            .as(transactionalOperator::transactional)
            .as(StepVerifier::create)
            .expectNext(document)
            .verifyComplete();

        reactiveTemplate
            .findById(500, SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result.getContent().equals("test1")).isTrue())
            .verifyComplete();
    }

    @Test
    public void verifyMultipleWritesInTransaction() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document1 = new SampleClasses.DocumentWithIntegerId(501, "test1");
        SampleClasses.DocumentWithIntegerId document2 = new SampleClasses.DocumentWithIntegerId(501, "test2");

        reactiveTemplate.insert(document1)
            .then(reactiveTemplate.save(document2))
            .then()
            .as(transactionalOperator::transactional)
            .as(StepVerifier::create)
            .verifyComplete();

        reactiveTemplate
            .findById(501, SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result.getContent().equals("test2")).isTrue())
            .verifyComplete();
    }

    @Test
    public void verifyMultipleWritesInTransactionWithTimeout() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document1 = new SampleClasses.DocumentWithIntegerId(520, "test1");
        SampleClasses.DocumentWithIntegerId document2 = new SampleClasses.DocumentWithIntegerId(520, "test2");

        reactiveTemplate.insert(document1)
            // wait less than the specified timeout for this transactional operator
            .delayElement(Duration.ofSeconds(1))
            .then(reactiveTemplate.save(document2))
            .then()
            .as(transactionalOperatorWithTimeout2::transactional)
            .as(StepVerifier::create)
            .verifyComplete();

        reactiveTemplate
            .findById(520, SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result.getContent().equals("test2")).isTrue())
            .verifyComplete();
    }

    @Test
    public void verifyMultipleWritesInTransactionWithTimeoutExpired() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document1 = new SampleClasses.DocumentWithIntegerId(521, "test1");
        SampleClasses.DocumentWithIntegerId document2 = new SampleClasses.DocumentWithIntegerId(521, "test2");

        reactiveTemplate.insert(document1)
            // wait more than the specified timeout for this transactional operator
            .delayElement(Duration.ofSeconds(3))
            .then(reactiveTemplate.save(document2))
            .then()
            .as(transactionalOperatorWithTimeout2::transactional)
            .as(StepVerifier::create)
            .verifyErrorMatches(throwable -> {
                if (throwable instanceof RecoverableDataAccessException) {
                    return throwable.getMessage().contains("MRT expired");
                }
                return false;
            });
    }

    @Test
    public void verifyMultipleWritesInTransactionWithDefaultTimeoutExpired() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document1 = new SampleClasses.DocumentWithIntegerId(522, "test1");
        SampleClasses.DocumentWithIntegerId document2 = new SampleClasses.DocumentWithIntegerId(522, "test2");

        reactiveTemplate.insert(document1)
            // wait more than the specified timeout for this transactional operator
            .delayElement(Duration.ofSeconds(15))
            .then(reactiveTemplate.save(document2))
            .then()
            .as(transactionalOperator::transactional)
            .as(StepVerifier::create)
            .verifyErrorMatches(throwable -> {
                if (throwable instanceof RecoverableDataAccessException) {
                    return throwable.getMessage().contains("MRT expired");
                }
                return false;
            });
    }

    @Test
    public void oneWriteInTransaction_manual_transactional() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document = new SampleClasses.DocumentWithIntegerId(502, "test1");

        transactionalOperator.transactional(reactiveTemplate.insert(document)).then()
            .as(StepVerifier::create)
            .verifyComplete();

        reactiveTemplate
            .findById(502, SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result.getContent().equals("test1")).isTrue())
            .verifyComplete();
    }

    @Test
    public void oneWriteInTransaction_manual_execute() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document = new SampleClasses.DocumentWithIntegerId(503, "test1");

        // Manually manage the transaction by using transactionalOperator.execute()
        transactionalOperator.execute(transaction -> {
                assertThat(transaction.isNewTransaction()).isTrue();
                assertThat(transaction.hasTransaction()).isTrue();
                assertThat(transaction.isCompleted()).isFalse();
                return reactiveTemplate.insert(document);
            }).then()
            .as(StepVerifier::create)
            .verifyComplete();

        reactiveTemplate
            .findById(503, SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result.getContent().equals("test1")).isTrue())
            .verifyComplete();
    }

    @Test
    public void multipleWritesInTransaction_manual_execute() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document1 = new SampleClasses.DocumentWithIntegerId(504, "test1");
        SampleClasses.DocumentWithIntegerId document2 = new SampleClasses.DocumentWithIntegerId(505, "test2");

        // Manually manage the transaction by using transactionalOperator.execute()
        transactionalOperator.execute(transaction -> {
                assertThat(transaction.isNewTransaction()).isTrue();
                assertThat(transaction.hasTransaction()).isTrue();
                assertThat(transaction.isCompleted()).isFalse();
                return reactiveTemplate.insert(document1).then(reactiveTemplate.save(document2));
            }).then()
            .as(StepVerifier::create)
            .verifyComplete();

        reactiveTemplate
            .count(SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result == 2).isTrue()) // both records were written
            .verifyComplete();
    }

    @Test
    public void verifyRepeatingCommit() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document1 = new SampleClasses.DocumentWithIntegerId(506, "test1");

        // Manually manage the transaction by using transactionalOperator.execute()
        transactionalOperator.execute(transaction -> {
                assertThat(transaction.isNewTransaction()).isTrue();
                assertThat(transaction.hasTransaction()).isTrue();
                assertThat(transaction.isCompleted()).isFalse();
                return reactiveTemplate.insert(document1)
                    // calling the first commit manually before end of transaction
                    .then(transactionManager.commit(transaction));
            }).then()
            .as(StepVerifier::create)
            .expectError(IllegalTransactionStateException.class);

        reactiveTemplate
            .findById(506, SampleClasses.DocumentWithIntegerId.class)
            .doOnSuccess(result -> {
                assertThat(result).isNull(); // rollback, nothing was written
            })
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    public void verifyTransactionRollback() {
        // Multi-record transactions are supported starting with Server version 8.0+
        SampleClasses.DocumentWithIntegerId document = new SampleClasses.DocumentWithIntegerId(507, "test1");

        reactiveTemplate.insert(document).then(reactiveTemplate.insert(document))
            .as(transactionalOperator::transactional) // Apply transactional context
            .as(StepVerifier::create)
            .expectError(DuplicateKeyException.class)
            .verify();

        reactiveTemplate
            .count(SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result == 0).isTrue()) // rollback, nothing was written
            .verifyComplete();
    }

    @Test
    public void oneWriteInTransaction_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+
        AtomicInteger counter = new AtomicInteger();
        int threadsNumber = 5;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            reactiveTemplate
                .insert(new SampleClasses.DocumentWithIntegerId(508 + counterValue, "test" + counterValue))
                .then()
                .as(transactionalOperator::transactional)
                .as(StepVerifier::create)
                .expectNext()
                .verifyComplete();
        });

        List<SampleClasses.DocumentWithIntegerId> results =
            reactiveTemplate.findAll(SampleClasses.DocumentWithIntegerId.class).collectList().block();
        assertThat(results).isNotNull();
        assertThat(results.size()).isEqualTo(threadsNumber);
    }

    @Test
    public void rollbackTransaction_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+
        AtomicInteger counter = new AtomicInteger();
        int threadsNumber = 5;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            reactiveTemplate
                .insert(new SampleClasses.DocumentWithIntegerId(509 + counterValue, "test" + counterValue))
                .then(reactiveTemplate.insert( // duplicate insert causes transaction rollback due to an exception
                    new SampleClasses.DocumentWithIntegerId(509 + counterValue, "test" + counterValue)))
                .then()
                .as(transactionalOperator::transactional)
                .as(StepVerifier::create)
                .expectError();
        });

        List<SampleClasses.DocumentWithIntegerId> results =
            reactiveTemplate.findAll(SampleClasses.DocumentWithIntegerId.class).collectList().block();
        assertThat(results).isNotNull();
        assertThat(results.isEmpty()).isTrue();
    }

    @Test
    public void multipleWritesInTransaction_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+
        AtomicInteger counter = new AtomicInteger();
        int threadsNumber = 5;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            reactiveTemplate
                .insert(new SampleClasses.DocumentWithIntegerId(510 + counterValue, "test" + counterValue))
                .then(reactiveTemplate.save(
                    new SampleClasses.DocumentWithIntegerId(510 + counterValue, "test" + counterValue)))
                .then()
                .as(transactionalOperator::transactional)
                .as(StepVerifier::create)
                .verifyComplete();
        });

        List<SampleClasses.DocumentWithIntegerId> results =
            reactiveTemplate.findAll(SampleClasses.DocumentWithIntegerId.class)
                .collectList().block();
        assertThat(results).isNotNull();
        assertThat(results.size()).isEqualTo(threadsNumber);
    }
}
