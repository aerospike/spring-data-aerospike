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
package org.springframework.data.aerospike.transaction.sync;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Txn;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.util.AwaitilityUtils;
import org.springframework.data.aerospike.util.TestUtils;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.transaction.sync.AerospikeTransactionTestUtils.callGetTransaction;
import static org.springframework.data.aerospike.transaction.sync.AerospikeTransactionTestUtils.getTransaction;
import static org.springframework.data.aerospike.transaction.sync.AerospikeTransactionTestUtils.getTransaction2;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTransactionalAnnotationTests extends BaseBlockingIntegrationTests {

    @BeforeAll
    public void beforeAll() {
        TestUtils.checkAssumption(serverVersionSupport.isTxnSupported(),
            "Skipping transactions tests because Aerospike Server 8.0.0+ is required", log);
    }

    @BeforeEach
    public void beforeEach() {
        deleteAll(Person.class, SampleClasses.DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    @AfterAll
    public void afterAll() {
        deleteAll(Person.class, SampleClasses.DocumentWithPrimitiveIntId.class,
            SampleClasses.DocumentWithIntegerId.class);
    }

    public void transactional_multipleInserts(Object document1, Object document2) {
        template.insert(document1);
        template.insert(document2);
    }

    @Test
    @Transactional(transactionManager = "aerospikeTransactionManager")
    @Rollback(value = false)
    public void verifyTransactionExists_oneMethod() {
        Txn tx = getTransaction(client);
        assertThat(tx).isNotNull();
    }

    @Test
    @Transactional(transactionManager = "aerospikeTransactionManager")
    @Rollback(value = false)
    public void verifyTransactionExists_chainedMethods() {
        Txn tx = callGetTransaction(client);
        assertThat(tx).isNotNull();
    }

    @Test
    @Transactional(transactionManager = "aerospikeTransactionManager")
    @Rollback(value = false)
    public void verifyTransactionExists_multipleMethods() {
        Txn tx1 = callGetTransaction(client);
        assertThat(tx1).isNotNull();
        Txn tx2 = getTransaction2(client);
        assertThat(tx2).isEqualTo(tx1);
    }

    @Test
    @Transactional()
    @Rollback(value = false)
    // only for testing purposes as performing one write in a transaction lacks sense
    public void verifyTransaction_oneInsert() {
        TestTransactionSynchronization testSync = new TestTransactionSynchronization(() -> {
            // findAll() is used here because it uses QueryPolicy and ignores transaction id
            // it is a callback after transaction completion, typically fast enough to happen before cleanup,
            // so de facto transaction id is often still findable at this point
            var resultsList = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class).toList();
            assertThat(resultsList).hasSize(1);
            assertThat(resultsList.stream().map(SampleClasses.DocumentWithPrimitiveIntId::getId).toList())
                .containsExactlyInAnyOrder(300);
            System.out.println("Verified");
        });
        // Register the action to perform after transaction is completed
        testSync.register();

        template.insert(new SampleClasses.DocumentWithPrimitiveIntId(300));
    }

    @Test
    @Transactional
    @Rollback(value = false)
    // just for testing purposes as performing only one write in a transactions lacks sense
    public void verifyTransaction_batchInsert() {
        TestTransactionSynchronization testSync = new TestTransactionSynchronization(() -> {
            var resultsList = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class).toList();
            assertThat(resultsList).hasSize(2);
            assertThat(resultsList.stream().map(SampleClasses.DocumentWithPrimitiveIntId::getId).toList())
                .containsExactlyInAnyOrder(301, 401);
            System.out.println("Verified");
        });
        // Register the action to perform after transaction is completed
        testSync.register();

        template.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(301),
            new SampleClasses.DocumentWithPrimitiveIntId(401)));
    }

    @Test
    @Transactional
    @Rollback(value = false)
    public void verifyTransaction_multipleWrites() {
        TestTransactionSynchronization testSync = new TestTransactionSynchronization(() -> {
            var resultsList = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class).toList();
            assertThat(resultsList).hasSize(2);
            assertThat(resultsList.stream().map(SampleClasses.DocumentWithPrimitiveIntId::getId).toList())
                .containsExactlyInAnyOrder(302, 402);
            System.out.println("Verified");
        });
        // Register the action to perform after transaction is completed
        testSync.register();

        transactional_multipleInserts(new SampleClasses.DocumentWithPrimitiveIntId(302),
            new SampleClasses.DocumentWithPrimitiveIntId(402));
    }

    @Test
    @Transactional(timeout = 2) // timeout after the first command within a transaction
    @Rollback(value = false)
    public void verifyTransaction_multipleInserts_withTimeout() {
        TestTransactionSynchronization testSync = new TestTransactionSynchronization(() -> {
            var resultsList = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class).toList();
            assertThat(resultsList).hasSize(2);
            assertThat(resultsList.stream().map(SampleClasses.DocumentWithPrimitiveIntId::getId).toList())
                .containsExactlyInAnyOrder(304, 305);
            System.out.println("Verified");
        });
        // Register the action to perform after transaction is completed
        testSync.register();

        template.insert(new SampleClasses.DocumentWithPrimitiveIntId(304));
        AwaitilityUtils.wait(1, SECONDS); // wait less than the given timeout
        template.insert(new SampleClasses.DocumentWithPrimitiveIntId(305));
    }

    @Test
    @Transactional(timeout = 2) // timeout after the first command within a transaction
    @Rollback(value = false)
    public void verifyTransaction_multipleInserts_withTimeoutExpired() {
        template.insert(new SampleClasses.DocumentWithPrimitiveIntId(305));
        AwaitilityUtils.wait(3, SECONDS); // wait more than the given timeout
        assertThatThrownBy(() -> template.insert(new SampleClasses.DocumentWithPrimitiveIntId(306)))
            .isInstanceOf(RecoverableDataAccessException.class)
            .hasMessageContaining("Transaction expired");
    }

    @Test
    @Transactional
    @Rollback() // rollback is set to true to simulate propagating exception that rolls back transaction
    public void verifyTransaction_multipleWrites_rollback() {
        TestTransactionSynchronization testSync = new TestTransactionSynchronization(() -> {
            var resultsList = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class).toList();
            assertThat(resultsList).hasSize(0);
            System.out.println("Verified");
        });
        // Register the action to perform after transaction is completed
        testSync.register();

        assertThatThrownBy(() ->
            transactional_multipleInserts(new SampleClasses.DocumentWithPrimitiveIntId(303),
                new SampleClasses.DocumentWithPrimitiveIntId(303)))
            .isInstanceOf(DuplicateKeyException.class)
            .hasMessageContaining("Key already exists");
    }

    @Test
    @Transactional(timeout = 2)
    @Rollback(value = false)
    public void verifyTransaction_multipleBatchInserts_withTimeout() {
        TestTransactionSynchronization testSync = new TestTransactionSynchronization(() -> {
            var resultsList = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class).toList();
            assertThat(resultsList).hasSize(4);
            assertThat(resultsList.stream().map(SampleClasses.DocumentWithPrimitiveIntId::getId).toList())
                .containsExactlyInAnyOrder(307, 407, 308, 408);
            System.out.println("Verified");
        });
        // Register the action to perform after transaction is completed
        testSync.register();

        template.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(307),
            new SampleClasses.DocumentWithPrimitiveIntId(407)));
        AwaitilityUtils.wait(1, SECONDS); // wait less than the given timeout
        template.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(308),
            new SampleClasses.DocumentWithPrimitiveIntId(408)));
    }

    @Test
    @Transactional(timeout = 2)
    @Rollback(value = false)
    public void verifyTransaction_multipleBatchInserts_withTimeoutExpired() {
        template.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(309),
            new SampleClasses.DocumentWithPrimitiveIntId(409)));
        AwaitilityUtils.wait(3, SECONDS); // wait more than the given timeout
        try {
            template.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(310),
                new SampleClasses.DocumentWithPrimitiveIntId(410)));
        } catch (AerospikeException.BatchRecordArray e) {
            System.out.println("Transaction expired");
        }
    }

    @Test
    @Transactional()
    @Rollback(value = false)
    // only for testing purposes as performing one write in a transaction lacks sense
    public void verifyTransaction_oneDelete() {
        TestTransactionSynchronization testSync = new TestTransactionSynchronization(() -> {
            var resultsList = template.findAll(SampleClasses.DocumentWithPrimitiveIntId.class).toList();
            assertThat(resultsList).hasSize(0);
            System.out.println("Verified");
        });
        // Register the action to perform after transaction is completed
        testSync.register();

        SampleClasses.DocumentWithPrimitiveIntId doc = new SampleClasses.DocumentWithPrimitiveIntId(1004);
        template.insert(doc);
        template.delete(doc);
    }
}
