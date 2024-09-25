package org.springframework.data.aerospike.transactions.reactive;

import com.aerospike.client.Txn;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionContextManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.transaction.reactive.TransactionSynchronizationManager.forCurrentTransaction;

public class ReactiveAerospikeTransactionTestUtils {

    private final IAerospikeReactorClient client;
    private final ReactiveAerospikeTemplate template;
    private final AerospikeReactiveTransactionManager txManager;

    public ReactiveAerospikeTransactionTestUtils(IAerospikeReactorClient client, ReactiveAerospikeTemplate template,
                                                 AerospikeReactiveTransactionManager txManager) {
        this.client = client;
        this.template = template;
        this.txManager = txManager;
    }

    protected Mono<Void> verifyOngoingTransaction_withPropagation(SampleClasses.DocumentWithPrimitiveIntId document,
                                                                  int propagationType, int numberOfSuspendCalls) {
        // Multi-record transactions are supported starting with Server version 8.0+
        var trackedTxManager = spy(txManager);
        var tranDefinition = new DefaultTransactionDefinition();
        tranDefinition.setPropagationBehavior(propagationType);
        var txOperator = TransactionalOperator.create(trackedTxManager, tranDefinition);

        return forCurrentTransaction()
            .flatMap(trSyncManager -> {
                var rHolder = new AerospikeReactiveTransactionResourceHolder(client);
                trSyncManager.bindResource(client, rHolder);
                return txOperator.execute(transaction -> template.insert(document)).then();
            })
            .contextWrite(TransactionContextManager.getOrCreateContext())
            .contextWrite(TransactionContextManager.getOrCreateContextHolder())
            .doOnNext(ignored -> verify(trackedTxManager, times(numberOfSuspendCalls)).doSuspend(any(), any()))
            .doOnNext(ignored -> template.count(SampleClasses.DocumentWithPrimitiveIntId.class)
                .map(count -> assertThat(count)
                    .withFailMessage("Record was not written")
                    .isEqualTo(1)))
            .then();
    }

    protected Mono<Txn> getTransaction() {
        return TransactionContextManager.currentContext()
            .flatMap(ctx -> {
                AerospikeReactiveTransactionResourceHolder resourceHolder =
                    (AerospikeReactiveTransactionResourceHolder) ctx.getResources().get(client);
                Txn tran = resourceHolder != null ?
                    resourceHolder.getTransaction() : null;
                return Mono.just(tran);
            })
            .onErrorResume(NoTransactionException.class, ignored ->
                Mono.empty());
    }

    protected <T> Mono<Void> performInTxAndVerifyCommitOnNext(ReactiveTransactionManager txManager,
                                                              TransactionalOperator txOperator, Mono<T> action) {
        action
            .as(txOperator::transactional)
            .as(StepVerifier::create)
            .consumeNextWith(item -> {
                // verify that commit() was called
                verify(txManager).commit(any());
            })
            .verifyComplete();

        return Mono.empty();
    }

    protected <T> Mono<Void> performInTxAndVerifyCommitOnComplete(ReactiveTransactionManager txManager,
                                                              TransactionalOperator txOperator, Mono<T> action) {
        action
            .as(txOperator::transactional)
            .doOnSuccess(i -> {
                // verify that commit() was called
                verify(txManager).commit(any());
            })
            .as(StepVerifier::create)
            .verifyComplete();

        return Mono.empty();
    }
}
