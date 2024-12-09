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

import com.aerospike.client.Txn;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithPrimitiveIntId;
import org.springframework.data.aerospike.util.TestUtils;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.NestedTransactionNotSupportedException;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.springframework.data.aerospike.transactions.sync.AerospikeTransactionTestUtils.getTransaction;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_MANDATORY;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_NESTED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_NEVER;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_NOT_SUPPORTED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_SUPPORTS;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class AerospikeTemplateTransactionUnitTests extends BaseBlockingIntegrationTests {

    @Autowired
    TransactionTemplate transactionTemplate;

    private AerospikeTransactionTestUtils utils;

    @BeforeAll
    public void beforeAll() {
        TestUtils.checkAssumption(serverVersionSupport.isMRTSupported(),
            "Skipping transactions tests because Aerospike Server 8.0.0+ is required", log);
        utils = new AerospikeTransactionTestUtils(client, template);
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
    public void verifyTransactionExists() {
        AtomicReference<Txn> tx = new AtomicReference<>();

        transactionTemplate.executeWithoutResult(status -> {
            tx.set(getTransaction(client));
        });

        Txn transaction = tx.get();
        assertThat(transaction).isNotNull();
        assertThat(transaction.getId()).isNotNull();
    }

    private <T> void performTxVerifyCommit(T documents, AerospikeTransactionTestUtils.TransactionAction<T> action) {
        AerospikeTransactionManager mockTxManager = mock(AerospikeTransactionManager.class);
        TransactionTemplate mockTxTemplate = new TransactionTemplate(mockTxManager);

        utils.performInTxAndVerifyCommit(mockTxManager, mockTxTemplate, documents, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void insertInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(200);
        performTxVerifyCommit(document, (argument, status) -> template.insert(document));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void insertAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents = List.of(new DocumentWithPrimitiveIntId(201));
        performTxVerifyCommit(documents, (argument, status) -> template.insertAll(documents));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void saveInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(202);
        performTxVerifyCommit(document, (argument, status) -> template.save(document));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void saveAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents = List.of(new DocumentWithPrimitiveIntId(203));
        performTxVerifyCommit(documents, (argument, status) -> template.saveAll(documents));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void updateInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(204);
        template.insert(document);
        performTxVerifyCommit(document, (argument, status) -> template.update(document));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void updateAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents = List.of(new DocumentWithPrimitiveIntId(205));
        template.insertAll(documents);
        performTxVerifyCommit(documents, (argument, status) -> template.updateAll(documents));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void addInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(206);
        performTxVerifyCommit(document, (argument, status) -> template.add(document, "bin", 206L));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void appendInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(207);
        performTxVerifyCommit(document, (argument, status) -> template.append(document, "bin", "test"));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void persistInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(208);
        performTxVerifyCommit(document, (argument, status) -> template.persist(document, client.getWritePolicyDefault()));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void findByIdInTransaction_verifyCommit() {
        int id = 209;
        performTxVerifyCommit(id, (argument, status) -> template.findById(id, DocumentWithPrimitiveIntId.class));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void findByIdsInTransaction_verifyCommit() {
        List<Integer> ids = List.of(210);
        performTxVerifyCommit(ids, (argument, status) -> template.findByIds(ids, DocumentWithPrimitiveIntId.class));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void findByGroupedEntitiesInTransaction_verifyCommit() {
        GroupedKeys groupedKeys = GroupedKeys.builder()
            .entityKeys(DocumentWithPrimitiveIntId.class, List.of(211))
            .build();
        performTxVerifyCommit(groupedKeys, (argument, status) -> template.findByIds(groupedKeys));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void existsInTransaction_verifyCommit() {
        int id = 212;
        performTxVerifyCommit(id, (argument, status) -> template.exists(id, DocumentWithPrimitiveIntId.class));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void deleteInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(213);
        template.insert(document);
        performTxVerifyCommit(document, (argument, status) -> template.delete(document));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void deleteAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents = List.of(new DocumentWithPrimitiveIntId(214));
        template.insertAll(documents);
        performTxVerifyCommit(documents, (argument, status) -> template.deleteAll(documents));
    }

    @Test
    public void ongoingTransactions_twoWrites_withPropagation_required() {
        // join an existing transaction if available, it is the default propagation level
        utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_REQUIRED, 1, 0);
    }

    @Test
    public void ongoingTransactions_twoWrites_withPropagation_requiresNew() {
        // always create a new transaction
        utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_REQUIRES_NEW, 2, 2);
    }

    @Test
    public void ongoingTransactions_twoWrites_withPropagation_supports() {
        // participate in a transaction, or if no transaction is present, run non-transactionally
        utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_SUPPORTS, 1, 0);
    }

    @Test
    public void ongoingTransactions_twoWrites_withPropagation_notSupported() {
        // execute non-transactionally, regardless of the presence of an active transaction;
        // if a transaction is already active, it will be suspended for the duration of the method execution
        utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_NOT_SUPPORTED, 0, 2);
    }

    @Test
    public void ongoingTransactions_twoWrites_withPropagation_mandatory() {
        // must run within an active transaction
        utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_MANDATORY, 1, 0);
    }

    @Test
    public void ongoingTransactions_twoWrites_withPropagation_never() {
        // never run within a transaction
        assertThatThrownBy(() ->
            utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_NEVER, 0, 0))
            .isInstanceOf(IllegalTransactionStateException.class)
            .hasMessageContaining("Existing transaction found for transaction marked with propagation 'never'");

        TransactionSynchronizationManager.unbindResource(client); // cleanup
        TransactionSynchronizationManager.clear();
    }

    @Test
    public void ongoingTransactions_twoWrites_withPropagation_nested() {
        // if a transaction exists, mark a savepoint to roll back to in case of an exception
        // nested transactions are not supported
        assertThatThrownBy(() ->
            utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_NESTED, 0, 0))
            .isInstanceOf(NestedTransactionNotSupportedException.class)
            .hasMessageContaining("Transaction manager does not allow nested transactions by default - " +
                "specify 'nestedTransactionAllowed' property with value 'true'");

        TransactionSynchronizationManager.unbindResource(client); // cleanup
        TransactionSynchronizationManager.clear();

        assertThatThrownBy(() ->
            utils.verifyTwoWritesEachInOngoingTransactionWithPropagation(PROPAGATION_NESTED, 1, 0, true))
            .isInstanceOf(NestedTransactionNotSupportedException.class)
            .hasMessageMatching("Transaction object .* does not support savepoints");

        TransactionSynchronizationManager.unbindResource(client); // cleanup
        TransactionSynchronizationManager.clear();
    }
}
