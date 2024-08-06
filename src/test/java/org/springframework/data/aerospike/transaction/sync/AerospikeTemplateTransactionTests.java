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
package org.springframework.data.aerospike.transaction.sync;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.util.AsyncUtils;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionDefinition;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateTransactionTests extends BaseBlockingIntegrationTests {

    @Autowired
    AerospikeTransactionManager transactionManager;

    @Autowired
    TransactionTemplate transactionTemplate;

    AerospikeTransactionManager mockTxManager = mock(AerospikeTransactionManager.class);
    TransactionTemplate mockTxTemplate = new TransactionTemplate(mockTxManager);

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
        mockTxTemplate.executeWithoutResult(status -> {
            template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
        });

        // verify that commit() was called
        verify(mockTxManager).commit(null);

        // resource holder must be already released
        assertThat(TransactionSynchronizationManager.getResource(client)).isNull();
    }

    @Test
    public void insertInTransaction_oneWrite() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insert(new SampleClasses.DocumentWithIntegerId(100, "test1"));
        });

        var result = template.findById(100, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result.getContent().equals("test1")).isTrue();
    }

    @Test
    public void insertInTransaction_multipleWrites() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insert(new SampleClasses.DocumentWithIntegerId(100, "test1"));
            template.save(new SampleClasses.DocumentWithIntegerId(100, "test2"));
        });

        var result = template.findById(100, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result.getContent().equals("test2")).isTrue();
    }

    @Test
    public void insertAllInTransaction_insertsAllDocuments() {
        // Multi-record transactions are supported starting with Server version 8.0+
//        if (serverVersionSupport.isMRTSupported()) { // TODO: uncomment when server 8 is released
        List<Person> persons = IntStream.range(1, 10)
            .mapToObj(age -> Person.builder().id(nextId())
                .firstName("Gregor")
                .age(age).build())
            .collect(Collectors.toList());

        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insertAll(persons);
        });

        var results = template.findAll(Person.class);
        assertThat(results.toList()).containsAll(persons);
//        }
    }

    @Test
    public void oneWriteInTransaction_rollbackWorks() {
        template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));

        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status ->
            template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100))))
            .isInstanceOf(DuplicateKeyException.class)
            .hasMessageContaining("Key already exists");

//        var results = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class); // TODO: check
//        assertThat(results.toList()).size().isEqualTo(1);
        var result = template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class);
        assertThat(result.getId()).isEqualTo(100);
    }

    @Test
    public void multipleWritesInTransaction_rollbackWorks() {
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
            template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
            template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
        }))
            .isInstanceOf(DuplicateKeyException.class)
            .hasMessageContaining("Key already exists");

        // No record is written because all inserts were in the same transaction
        assertThat(template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class)).isNull();
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
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
            }
        });

        transactionManager.commit(transactionStatus);
        assertThat(template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class)).isNotNull();
    }

    @Test
    public void ongoingTransaction_repeatingCommit() {
        // initialize a new transaction with an empty resource holder
        // subsequently doBegin() gets called which adds a new resource holder to the transaction
        TransactionStatus transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);

        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
            }
        });

        transactionManager.commit(transactionStatus);
        assertThatThrownBy(() -> transactionManager.commit(transactionStatus))
            .isInstanceOf(IllegalTransactionStateException.class)
            .hasMessageContaining("Transaction is already completed");
        assertThat(template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class)).isNotNull();
    }

    @Test
    public void ongoingTransaction_commit_existingTransaction() {
        // in the regular flow binding a resource is done within doBegin() in AerospikeTransactionManager
        var resourceHolder = new AerospikeTransactionResourceHolder(client);
        TransactionSynchronizationManager.bindResource(client, resourceHolder);

        // initialize a new transaction with the existing resource holder if it is already bound
        TransactionStatus transactionStatus = transactionManager.getTransaction(new DefaultTransactionDefinition());
        resourceHolder.setSynchronizedWithTransaction(true);
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);


        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            protected void doInTransactionWithoutResult(TransactionStatus status) {
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
            }
        });

        transactionManager.commit(transactionStatus);

        TransactionSynchronizationManager.unbindResource(client); // cleanup
        TransactionSynchronizationManager.clear();
        resourceHolder.clear();

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
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
                status.setRollbackOnly(); // set rollbackOnly
            }
        });

        assertThatThrownBy(() -> transactionManager.commit(transactionStatus))
            .isInstanceOf(UnexpectedRollbackException.class);
        assertThat(template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class)).isNull();
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
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
            }
        });

        transactionManager.rollback(transactionStatus);
        assertThat(template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class)).isNull();
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
                template.insert(new SampleClasses.DocumentWithPrimitiveIntId(100));
            }
        });

        transactionManager.rollback(transactionStatus);
        assertThatThrownBy(() -> transactionManager.rollback(transactionStatus))
            .isInstanceOf(IllegalTransactionStateException.class)
            .hasMessageContaining("Transaction is already completed");
        assertThat(template.findById(100, SampleClasses.DocumentWithPrimitiveIntId.class)).isNull();
    }

    @Test
    public void insertInTransaction_oneWrite_withPropagation() {
        // Multi-record transactions are supported starting with Server version 8.0+
        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
//        transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_NEVER);

        transactionTemplate.executeWithoutResult(status -> {
            assertThat(status.isNewTransaction()).isTrue();
            template.insert(new SampleClasses.DocumentWithIntegerId(100, "test1"));
        });

        var result = template.findById(100, SampleClasses.DocumentWithIntegerId.class);
        assertThat(result.getContent().equals("test1")).isTrue();
    }

    @Test
    public void insertInTransaction_oneWrite_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+

        var counter = new AtomicInteger();
        int threadsNumber = 5;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            transactionTemplate.executeWithoutResult(status -> {
                assertThat(status.isNewTransaction()).isTrue();
                template.insert(
                    new SampleClasses.DocumentWithIntegerId(100 + counterValue, "test" + counterValue));
            });
        });

        var results = template.findAll(SampleClasses.DocumentWithIntegerId.class).toList();
        assertThat(results.size() == threadsNumber).isTrue();
    }

    @Test
    public void insertInTransaction_rollback_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+
        var counter = new AtomicInteger();
        int threadsNumber = 10;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(status -> {
                template.insert(new SampleClasses.DocumentWithIntegerId(100 + counterValue, "test" + counterValue));
                template.insert(new SampleClasses.DocumentWithIntegerId(100 + counterValue, "test" + counterValue));
            }))
                .isInstanceOf(DuplicateKeyException.class)
                .hasMessageContaining("Key already exists");
        });

        var results = template.findAll(SampleClasses.DocumentWithIntegerId.class).toList();
        assertThat(results.isEmpty()).isTrue();
    }

    @Test
    public void insertInTransaction_multipleWrites_multipleThreads() {
        // Multi-record transactions are supported starting with Server version 8.0+

        var counter = new AtomicInteger();
        int threadsNumber = 10;
        AsyncUtils.executeConcurrently(threadsNumber, () -> {
            int counterValue = counter.incrementAndGet();
            transactionTemplate.executeWithoutResult(status -> {
                assertThat(status.isNewTransaction()).isTrue();
                template.insert(
                    new SampleClasses.DocumentWithIntegerId(100 + counterValue, "test" + counterValue));
                template.save(
                    new SampleClasses.DocumentWithIntegerId(100 + counterValue, "test_`" + counterValue));
            });
        });

        var results = template.findAll(SampleClasses.DocumentWithIntegerId.class).toList();
        assertThat(results.size() == threadsNumber).isTrue();
    }
}
