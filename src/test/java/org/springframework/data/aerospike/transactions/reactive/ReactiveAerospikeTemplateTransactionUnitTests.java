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

import com.aerospike.client.policy.WritePolicy;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.sample.SampleClasses.DocumentWithPrimitiveIntId;
import org.springframework.data.aerospike.util.TestUtils;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_MANDATORY;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_NESTED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_NEVER;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_NOT_SUPPORTED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW;
import static org.springframework.transaction.TransactionDefinition.PROPAGATION_SUPPORTS;
import static org.springframework.transaction.reactive.TransactionSynchronizationManager.forCurrentTransaction;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ReactiveAerospikeTemplateTransactionUnitTests extends BaseReactiveIntegrationTests {

    @Autowired
    AerospikeReactiveTransactionManager transactionManager;

    @Autowired
    TransactionalOperator transactionalOperator;

    AerospikeReactiveTransactionManager mockTxManager = mock(AerospikeReactiveTransactionManager.class);
    TransactionalOperator mockTxOperator =
        TransactionalOperator.create(mockTxManager, new DefaultTransactionDefinition());
    private ReactiveAerospikeTransactionTestUtils utils;

    @BeforeAll
    public void beforeAll() {
        TestUtils.checkAssumption(serverVersionSupport.isMRTSupported(),
            "Skipping transactions tests because Aerospike Server 8.0.0+ is required", log);
        when(mockTxManager.getReactiveTransaction(any()))
            .thenReturn(Mono.just(
                new GenericReactiveTransaction("name", new AerospikeReactiveTransaction(null),
                    true, true, false, false, false, null)
            ));
        when(mockTxManager.commit(any())).thenReturn(Mono.empty());
        when(mockTxManager.rollback(any())).thenReturn(Mono.empty());
        utils = new ReactiveAerospikeTransactionTestUtils(reactiveClient, reactiveTemplate, transactionManager);
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
    public void writeInTransaction_verifyCommit() {
        reactiveTemplate.insert(new DocumentWithPrimitiveIntId(100)).then()
            .as(mockTxOperator::transactional)
            .as(StepVerifier::create)
            .verifyComplete();

        // verify that commit() was called
        verify(mockTxManager).commit(any(GenericReactiveTransaction.class));

        // resource holder must be already released
        assertThat(TransactionSynchronizationManager.getResource(reactiveClient)).isNull();
    }

    @Test
    public void verifyTransactionExists() {
        utils.getTransaction()
            .as(transactionalOperator::transactional)
            .as(StepVerifier::create)
            .consumeNextWith(tran -> {
                assertThat(tran).isNotNull();
                assertThat(tran.getId()).isNotNull();
            })
            .verifyComplete();
    }

    private <T> void performTxVerifyCommit(Object documents, Mono<T> action) {
        AerospikeReactiveTransactionManager trackedTxManager = spy(transactionManager);
        TransactionalOperator txOperator = TransactionalOperator.create(trackedTxManager);

        utils.performInTxAndVerifyCommit(trackedTxManager, txOperator, action.flatMap(i -> Mono.just(documents)));
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void insertInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(100);
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.insert(document);

        performTxVerifyCommit(document, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void insertAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents =
            List.of(new DocumentWithPrimitiveIntId(100));
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.insertAll(documents).next();

        performTxVerifyCommit(documents, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void saveInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(100);
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.save(document);

        performTxVerifyCommit(document, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void saveAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents =
            List.of(new DocumentWithPrimitiveIntId(100));
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.saveAll(documents).next();

        performTxVerifyCommit(documents, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void updateInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(100);
        reactiveTemplate.insert(document).block();
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.update(document);

        performTxVerifyCommit(document, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void updateAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents =
            List.of(new DocumentWithPrimitiveIntId(100));
        reactiveTemplate.insertAll(documents).blockLast();
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.updateAll(documents).next();

        performTxVerifyCommit(documents, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void addInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(100);
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.add(document, "bin", 100L);

        performTxVerifyCommit(document, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void appendInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(100);
        Mono<DocumentWithPrimitiveIntId> action = reactiveTemplate.append(document, "bin", "test");

        performTxVerifyCommit(document, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void persistInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(100);
        Mono<DocumentWithPrimitiveIntId> action =
            reactiveTemplate.persist(document, reactiveClient.getWritePolicyDefault());

        performTxVerifyCommit(document, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void findByIdInTransaction_verifyCommit() {
        int id = 100;
        reactiveTemplate.insert(new DocumentWithPrimitiveIntId(id)).block();
        Mono<DocumentWithPrimitiveIntId> action =
            reactiveTemplate.findById(id, DocumentWithPrimitiveIntId.class);

        performTxVerifyCommit(id, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void findByIdsInTransaction_verifyCommit() {
        int id = 100;
        List<Integer> ids = List.of(id);
        reactiveTemplate.insert(new DocumentWithPrimitiveIntId(id)).block();
        Mono<DocumentWithPrimitiveIntId> action =
            reactiveTemplate.findByIds(ids, DocumentWithPrimitiveIntId.class).next();

        performTxVerifyCommit(id, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void findByGroupedEntitiesInTransaction_verifyCommit() {
        GroupedKeys groupedKeys = GroupedKeys.builder()
            .entityKeys(DocumentWithPrimitiveIntId.class, List.of(100))
            .build();
        int id = 100;
        reactiveTemplate.insert(new DocumentWithPrimitiveIntId(id)).block();
        Mono<GroupedEntities> action = reactiveTemplate.findByIds(groupedKeys);

        performTxVerifyCommit(id, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void existsInTransaction_verifyCommit() {
        int id = 100;
        reactiveTemplate.insert(new DocumentWithPrimitiveIntId(id)).block();
        Mono<Boolean> action =
            reactiveTemplate.exists(id, DocumentWithPrimitiveIntId.class);

        performTxVerifyCommit(id, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void deleteInTransaction_verifyCommit() {
        DocumentWithPrimitiveIntId document = new DocumentWithPrimitiveIntId(100);
        reactiveTemplate.insert(document).block();
        Mono<Boolean> action = reactiveTemplate.delete(document);

        performTxVerifyCommit(document, action);
    }

    @Test
    // just for testing purposes as performing only one operation in a transaction lacks sense
    public void deleteAllInTransaction_verifyCommit() {
        List<DocumentWithPrimitiveIntId> documents =
            List.of(new DocumentWithPrimitiveIntId(100));
        reactiveTemplate.insertAll(documents).blockLast();
        Mono<Void> action = reactiveTemplate.deleteAll(documents);

        performTxVerifyCommit(documents, action);
    }

    @Test
    public void verifyOngoingTransaction_withPropagation_required() {
        // join an existing transaction if available, it is the default propagation level
        utils.verifyOngoingTransaction_withPropagation(PROPAGATION_REQUIRED, 0)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    public void verifyOngoingTransaction_withPropagation_requiresNew() {
        // always create a new transaction
        utils.verifyOngoingTransaction_withPropagation(PROPAGATION_REQUIRES_NEW, 1)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    public void verifyOngoingTransaction_withPropagation_supports() {
        // participate in a transaction, or if no transaction is present, run non-transactionally
        utils.verifyOngoingTransaction_withPropagation(PROPAGATION_SUPPORTS, 0)
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    public void verifyOngoingTransaction_withPropagation_notSupported() {
        // execute non-transactionally, regardless of the presence of an active transaction;
        // if a transaction is already active, it will be suspended for the duration of the method execution
        utils.verifyOngoingTransaction_withPropagation(PROPAGATION_NOT_SUPPORTED, 1)
            .as(StepVerifier::create)
            .verifyComplete();
        ;
    }

    @Test
    public void verifyOngoingTransaction_withPropagation_mandatory() {
        // must run within an active transaction
        utils.verifyOngoingTransaction_withPropagation(PROPAGATION_MANDATORY, 0)
            .as(StepVerifier::create)
            .verifyComplete();
        ;
    }

    @Test
    public void verifyOngoingTransaction_withPropagation_never() {
        // never run within a transaction
        utils.verifyOngoingTransaction_withPropagation(PROPAGATION_NEVER, 0)
            .as(StepVerifier::create)
            .expectErrorMatches(e -> {
                if (!(e instanceof IllegalTransactionStateException)) {
                    return false;
                }

                String errMsg = "Existing transaction found for transaction marked with propagation 'never'";
                return errMsg.equals(e.getMessage());
            })
            .verify();
    }

    @Test
    public void verifyOngoingTransaction_withPropagation_nested() {
        // if a transaction exists, mark a savepoint to roll back to in case of an exception
        // nested transactions are not supported
        utils.verifyOngoingTransaction_withPropagation(PROPAGATION_NESTED, 0)
            .as(StepVerifier::create)
            .expectErrorMatches(e -> {
                if (!(e instanceof TransactionSystemException)) {
                    return false;
                }

                String errMsg = "Could not bind transaction resource";
                return e.getMessage().contains(errMsg);
            })
            .verify();
    }

    @Test
    public void nativeSessionSynchronization_verifyTransactionProperties() {
        transactionalOperator
            .execute(transaction -> forCurrentTransaction()
                .doOnNext(synchronizationManager -> {
                    assertThat(synchronizationManager.isSynchronizationActive()).isTrue();
                    assertThat(transaction.isNewTransaction()).isTrue();
                    assertThat(synchronizationManager.hasResource(reactiveClient)).isTrue();

                    assertThat(transaction.isRollbackOnly()).isFalse();
                    assertThat(transaction.isCompleted()).isFalse();
                    assertThat(transaction.isNested()).isFalse();
                }))
            .then()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    public void nativeSessionSynchronization_verifyTransactionExists() {
        transactionalOperator
            .execute(transaction -> forCurrentTransaction()
                .doOnNext(synchronizationManager -> {
                    WritePolicy wPolicy = reactiveClient.getWritePolicyDefault();
                    AerospikeReactiveTransactionResourceHolder rHolder =
                        (AerospikeReactiveTransactionResourceHolder) synchronizationManager.getResource(reactiveClient);
                    wPolicy.txn = rHolder != null ? rHolder.getTransaction() : null;
                    assertThat(wPolicy.txn).isNotNull().isEqualTo(rHolder.getTransaction());
                })).then()
            .as(StepVerifier::create)
            .verifyComplete();

        reactiveTemplate
            .count(SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result == 1).isTrue()) // rollback, nothing was written
            .verifyComplete();
    }

    @Test
    public void nativeSessionSynchronization_verifySetRollbackOnly() {
        transactionalOperator
            .execute(transaction -> {
                transaction.setRollbackOnly();
                return forCurrentTransaction()
                    .doOnNext(synchronizationManager -> {
                        assertThat(synchronizationManager.isSynchronizationActive()).isTrue();
                        assertThat(transaction.isNewTransaction()).isTrue();
                        assertThat(synchronizationManager.hasResource(reactiveClient)).isTrue();
                        assertThat(transaction.isRollbackOnly()).isTrue();
                        assertThat(transaction.isRollbackOnly()).isTrue();
                    });
            })
            .then()
            .as(StepVerifier::create)
            .verifyComplete();
    }

    @Test
    public void nativeSessionSynchronization_verifyRollback() {
        SampleClasses.DocumentWithIntegerId document = new SampleClasses.DocumentWithIntegerId(100, "test1");
        transactionalOperator
            .execute(transaction -> forCurrentTransaction()
                .doOnNext(synchronizationManager -> {
                    WritePolicy wPolicy = reactiveClient.getWritePolicyDefault();
                    AerospikeReactiveTransactionResourceHolder rHolder =
                        (AerospikeReactiveTransactionResourceHolder) synchronizationManager.getResource(reactiveClient);
                    wPolicy.txn = rHolder != null ? rHolder.getTransaction() : null;
                    assertThat(wPolicy.txn).isNotNull().isEqualTo(rHolder.getTransaction());

                    reactiveTemplate.insert(document)
                        .then(reactiveTemplate.insert(document)) // duplicate insert generates an exception
                        .as(StepVerifier::create)
                        .expectError();
                })).then()
            .as(StepVerifier::create)
            .verifyComplete();

        reactiveTemplate
            .count(SampleClasses.DocumentWithIntegerId.class)
            .as(StepVerifier::create)
            .consumeNextWith(result -> assertThat(result == 0).isTrue()) // rollback, nothing was written
            .verifyComplete();
    }
}
