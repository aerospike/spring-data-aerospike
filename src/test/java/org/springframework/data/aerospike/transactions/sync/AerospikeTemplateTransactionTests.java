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
package org.springframework.data.aerospike.transactions.sync;

import com.aerospike.client.AerospikeException;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithPrimitiveIntId;
import org.springframework.data.aerospike.util.AsyncUtils;
import org.springframework.data.aerospike.util.AwaitilityUtils;
import org.springframework.data.aerospike.util.TestUtils;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.UnexpectedRollbackException;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateTransactionTests extends BaseBlockingIntegrationTests {

    @Autowired
    AerospikeTransactionManager transactionManager;

    @Autowired
    TransactionTemplate transactionTemplate;

    @BeforeAll
    public void beforeAll() {
        TestUtils.checkAssumption(serverVersionSupport.isMRTSupported(),
            "Skipping transactions tests because Aerospike Server 8.0.0+ is required", log);
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
    // just for testing purposes as performing only one write in a transaction lacks sense
    public void writeInTransaction() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insert(new SampleClasses.DocumentWithIntegerId(107, "test1"));
        });

        SampleClasses.DocumentWithIntegerId result = template.findById(107, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result.getContent().equals("test1")).isTrue();
    }

    @Test
    public void multipleWritesInTransaction() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insert(new SampleClasses.DocumentWithIntegerId(101, "test1"));
            template.save(new SampleClasses.DocumentWithIntegerId(101, "test2"));
        });

        SampleClasses.DocumentWithIntegerId result = template.findById(101, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result.getContent().equals("test2")).isTrue();
    }

    @Test
    public void multipleWritesInTransactionWithTimeout() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.setTimeout(2); // timeout after the first command within a transaction
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insert(new SampleClasses.DocumentWithIntegerId(114, "test1"));
            AwaitilityUtils.wait(1, SECONDS); // timeout does not expire during this wait
            template.save(new SampleClasses.DocumentWithIntegerId(114, "test2"));
        });

        SampleClasses.DocumentWithIntegerId result =
            template.findById(114, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result.getContent().equals("test2")).isTrue();
    }

    @Test
    public void multipleWritesInTransactionWithTimeoutExpired() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.setTimeout(2); // timeout after the first command within a transaction
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            template.insert(new SampleClasses.DocumentWithIntegerId(115, "test1"));
            AwaitilityUtils.wait(5, SECONDS); // timeout expires during this wait
            template.save(new SampleClasses.DocumentWithIntegerId(115, "test2"));
        }))
            .isInstanceOf(RecoverableDataAccessException.class)
            .hasMessageContaining("MRT expired");

        SampleClasses.DocumentWithIntegerId result = template.findById(115, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result).isNull(); // No record is written because all commands were in the same transaction
    }

    @Test
    // just for testing purposes as performing only one write in a transaction lacks sense
    public void batchWriteInTransaction() {
        // Multi-record transactions are supported starting with Server version 8.0+
        List<Person> persons = IntStream.range(1, 10)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());

        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insertAll(persons);
        });

        Stream<Person> results = template.findAll(Person.class);
        assertThat(results.toList()).containsAll(persons);
    }

    @Test
    public void multipleBatchWritesInTransactionWithTimeout() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.setTimeout(2); // timeout after the first command within a transaction
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insertAll(List.of(new DocumentWithPrimitiveIntId(116),
                new DocumentWithPrimitiveIntId(117)));
            AwaitilityUtils.wait(1, SECONDS); // timeout does not expire during this wait
            template.insertAll(List.of(new DocumentWithPrimitiveIntId(118),
                new DocumentWithPrimitiveIntId(119)));
        });

        assertThat(template.findById(116, DocumentWithPrimitiveIntId.class)).isNotNull();
        assertThat(template.findById(117, DocumentWithPrimitiveIntId.class)).isNotNull();
        assertThat(template.findById(118, DocumentWithPrimitiveIntId.class)).isNotNull();
        assertThat(template.findById(119, DocumentWithPrimitiveIntId.class)).isNotNull();
    }

    @Test
    public void multipleBatchWritesInTransactionWithTimeoutExpired() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.setTimeout(2); // timeout after the first command within a transaction
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            template.insertAll(List.of(new DocumentWithPrimitiveIntId(120),
                new DocumentWithPrimitiveIntId(121)));
            AwaitilityUtils.wait(3, SECONDS); // timeout expires during this wait
            template.insertAll(List.of(new DocumentWithPrimitiveIntId(122),
                new DocumentWithPrimitiveIntId(123)));
        }))
            .isInstanceOf(AerospikeException.BatchRecordArray.class)
            .hasMessageContaining("Batch failed");

        SampleClasses.DocumentWithIntegerId result =
            template.findById(120, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result).isNull(); // No record is written because of rollback of the transaction
    }

    @Test
    public void oneWriteInTransaction_rollback() {
        template.insert(new DocumentWithPrimitiveIntId(102));

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status ->
            template.insert(new DocumentWithPrimitiveIntId(102))))
            .isInstanceOf(DuplicateKeyException.class)
            .hasMessageContaining("Key already exists");

        DocumentWithPrimitiveIntId result = template.findById(102,
            DocumentWithPrimitiveIntId.class);
        assertThat(result.getId()).isEqualTo(102);
    }

    @Test
    public void multipleWritesInTransaction_rollback() {
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            template.insert(new DocumentWithPrimitiveIntId(103));
            template.insert(new DocumentWithPrimitiveIntId(103));
        }))
            .isInstanceOf(DuplicateKeyException.class)
            .hasMessageContaining("Key already exists");

        // No record is written because all inserts were in the same transaction
        assertThat(template.findById(103, DocumentWithPrimitiveIntId.class)).isNull();
    }

    @Test
    public void ongoingTransaction_commit() {
        // initialize a new transaction with an empty resource holder
        // subsequently doBegin() gets called which adds a new resource holder to the transaction
        TransactionStatus transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new DocumentWithPrimitiveIntId(104));
                AwaitilityUtils.wait(5, SECONDS);
            }
        });

        transactionManager.commit(transactionStatus);
        assertThat(template.findById(104, DocumentWithPrimitiveIntId.class)).isNotNull();
    }

    @Test
    public void ongoingTransaction_repeatingCommit() {
        // initialize a new transaction with an empty resource holder
        // subsequently doBegin() gets called which adds a new resource holder to the transaction
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new DocumentWithPrimitiveIntId(105));
            }
        });

        TransactionStatus transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
        transactionManager.commit(transactionStatus);
        assertThatThrownBy(() -> transactionManager.commit(transactionStatus))
            .isInstanceOf(IllegalTransactionStateException.class)
            .hasMessageContaining("Transaction is already completed");
        assertThat(template.findById(105, DocumentWithPrimitiveIntId.class)).isNotNull();
    }

    @Test
    public void ongoingTransaction_commit_existingTransaction() {
        // in the regular flow binding a resource is done within doBegin() in AerospikeTransactionManager
        // binding resource holder manually here to make getTransaction() recognize an ongoing transaction
        // and not to start a new transaction automatically in order to be able to start it manually later
        AerospikeTransactionResourceHolder resourceHolder = new AerospikeTransactionResourceHolder(client);
        TransactionSynchronizationManager.bindResource(client, resourceHolder);

        // initialize a new transaction with the existing resource holder if it is already bound
        TransactionStatus transactionStatus = spy(
            transactionManager.getTransaction(new DefaultTransactionDefinition()));
        resourceHolder.setSynchronizedWithTransaction(true);

        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new DocumentWithPrimitiveIntId(106));
            }
        });

        // changing the status manually here to simulate a new transaction
        // because otherwise with an ongoing transaction (isNextTransaction() == false) and the default propagation
        // doBegin() and doCommit() are not called automatically waiting to participate in the ongoing transaction
        when(transactionStatus.isNewTransaction()).thenReturn(true);
        transactionManager.commit(transactionStatus);
        assertThat(template.findById(106, DocumentWithPrimitiveIntId.class).getId())
            .isEqualTo(106);
    }

    @Test
    public void ongoingTransaction_withStatusRollbackOnly() {
        // initialize a new transaction with an empty resource holder
        // subsequently doBegin() gets called which adds a new resource holder to the transaction
        TransactionStatus transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new DocumentWithPrimitiveIntId(108));
                status.setRollbackOnly(); // set rollbackOnly
            }
        });

        assertThatThrownBy(() -> transactionManager.commit(transactionStatus))
            .isInstanceOf(UnexpectedRollbackException.class);
        assertThat(template.findById(108, DocumentWithPrimitiveIntId.class)).isNull();
    }

    @Test
    public void ongoingTransaction_rollback() {
        // initialize a new transaction with an empty resource holder
        // subsequently doBegin() gets called which adds a new resource holder to the transaction
        TransactionStatus transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new DocumentWithPrimitiveIntId(109));
            }
        });

        transactionManager.rollback(transactionStatus);
        assertThat(template.findById(109, DocumentWithPrimitiveIntId.class)).isNull();
    }

    @Test
    public void ongoingTransaction_repeatingRollback() {
        // initialize a new transaction with an empty resource holder
        // subsequently doBegin() gets called which adds a new resource holder to the transaction
        TransactionStatus transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new DocumentWithPrimitiveIntId(110));
            }
        });

        transactionManager.rollback(transactionStatus);
        assertThatThrownBy(() -> transactionManager.rollback(transactionStatus))
            .isInstanceOf(IllegalTransactionStateException.class)
            .hasMessageContaining("Transaction is already completed");
        assertThat(template.findById(110, DocumentWithPrimitiveIntId.class)).isNull();
    }

    @Test
    public void oneWriteInTransaction_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+
        AtomicInteger counter = new AtomicInteger();
        int threadsNumber = 5;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            transactionTemplate.executeWithoutResult(status -> {
                assertThat(status.isNewTransaction()).isTrue();
                template.insert(
                    new SampleClasses.DocumentWithIntegerId(111 + counterValue, "test" + counterValue));
            });
        });

        List<SampleClasses.DocumentWithIntegerId> results =
            template.findAll(SampleClasses.DocumentWithIntegerId.class).toList();
        assertThat(results.size() == threadsNumber).isTrue();
    }

    @Test
    public void rollbackTransaction_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+
        AtomicInteger counter = new AtomicInteger();
        int threadsNumber = 10;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
                template.insert(new SampleClasses.DocumentWithIntegerId(112 + counterValue, "test" + counterValue));
                template.insert(new SampleClasses.DocumentWithIntegerId(112 + counterValue, "test" + counterValue));
            }))
                .isInstanceOf(DuplicateKeyException.class)
                .hasMessageContaining("Key already exists");
        });

        List<SampleClasses.DocumentWithIntegerId> results =
            template.findAll(SampleClasses.DocumentWithIntegerId.class).toList();
        assertThat(results.isEmpty()).isTrue();
    }

    @Test
    public void multipleWritesInTransaction_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+
        AtomicInteger counter = new AtomicInteger();
        int threadsNumber = 10;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            transactionTemplate.executeWithoutResult(status -> {
                assertThat(status.isNewTransaction()).isTrue();
                template.insert(
                    new SampleClasses.DocumentWithIntegerId(113 + counterValue, "test" + counterValue));
                template.save(
                    new SampleClasses.DocumentWithIntegerId(113 + counterValue, "test_`" + counterValue));
            });
        });

        List<SampleClasses.DocumentWithIntegerId> results =
            template.findAll(SampleClasses.DocumentWithIntegerId.class).toList();
        assertThat(results.size() == threadsNumber).isTrue();
    }

    @Test
    // just for testing purposes as performing only one write in a transaction lacks sense
    public void deleteInTransaction() {
        DocumentWithPrimitiveIntId doc = new DocumentWithPrimitiveIntId(1000);
        template.insert(doc);
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.delete(doc);
        });

        DocumentWithPrimitiveIntId result =
            template.findById(1000, DocumentWithPrimitiveIntId.class);
        assertThat(result).isNull();
    }
}
