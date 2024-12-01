package org.springframework.data.aerospike.transaction.reactive;

import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.Getter;
import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;

import static org.springframework.data.aerospike.transaction.reactive.AerospikeReactiveTransactionResourceHolder.determineTimeout;

/**
 * A {@link org.springframework.transaction.ReactiveTransactionManager} implementation for managing transactions
 */
@Getter
public class AerospikeReactiveTransactionManager extends AbstractReactiveTransactionManager {

    private final IAerospikeReactorClient client;

    /**
     * Create a new instance of {@link AerospikeReactiveTransactionManager}
     */

    public AerospikeReactiveTransactionManager(IAerospikeReactorClient client) {
        this.client = client;
    }

    private static AerospikeReactiveTransaction toAerospikeTransaction(Object transaction) {
        Assert.isInstanceOf(AerospikeReactiveTransaction.class, transaction,
            () -> String.format("Expected to find instance of %s but instead found %s",
                AerospikeReactiveTransaction.class, transaction.getClass()));

        return (AerospikeReactiveTransaction) transaction;
    }

    private static AerospikeReactiveTransaction getTransaction(GenericReactiveTransaction status) {
        Assert.isInstanceOf(AerospikeReactiveTransaction.class, status.getTransaction(),
            () -> String.format("Expected to find instance of %s but instead found %s",
                AerospikeReactiveTransaction.class, status.getTransaction().getClass()));

        return (AerospikeReactiveTransaction) status.getTransaction();
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        return toAerospikeTransaction(transaction).hasResourceHolder();
    }

    @Override
    protected Object doGetTransaction(TransactionSynchronizationManager synchronizationManager) {
        AerospikeReactiveTransactionResourceHolder resourceHolder =
            (AerospikeReactiveTransactionResourceHolder) synchronizationManager.getResource(client);
        return new AerospikeReactiveTransaction(resourceHolder);
    }

    @Override
    protected Mono<Void> doBegin(TransactionSynchronizationManager synchronizationManager, Object transaction,
                                 TransactionDefinition definition) {
        return Mono.defer(() -> {
            AerospikeReactiveTransaction aerospikeTransaction = toAerospikeTransaction(transaction);
            // create new resourceHolder with a new Tran, de facto start transaction
            Mono<AerospikeReactiveTransactionResourceHolder> resourceHolder = createResourceHolder(client, definition);

            return resourceHolder
                .doOnNext(aerospikeTransaction::setResourceHolder)
                .onErrorMap(e -> new TransactionSystemException("Could not start transaction", e))
                .doOnSuccess(rHolder -> {
                    rHolder.setSynchronizedWithTransaction(true);
                    synchronizationManager.bindResource(client, rHolder);
                })
                .onErrorMap(e -> new TransactionSystemException("Could not bind transaction resource", e))
                .then();
        });
    }

    private Mono<AerospikeReactiveTransactionResourceHolder> createResourceHolder(IAerospikeReactorClient client,
                                                                                  TransactionDefinition definition) {
        AerospikeReactiveTransactionResourceHolder resourceHolder =
            new AerospikeReactiveTransactionResourceHolder(client);
        resourceHolder.setTimeoutIfNotDefault(determineTimeout(definition));
        return Mono.just(resourceHolder);
    }

    @Override
    protected Mono<Void> doCommit(TransactionSynchronizationManager synchronizationManager,
                                  GenericReactiveTransaction status) {
        return Mono.fromRunnable(() -> {
                AerospikeReactiveTransaction transaction = getTransaction(status);
                transaction.commitTransaction();
            })
            .onErrorMap(e -> new TransactionSystemException("Could not commit transaction", e))
            .then();
    }

    @Override
    protected Mono<Void> doRollback(TransactionSynchronizationManager synchronizationManager,
                                    GenericReactiveTransaction status) {
        return Mono.fromRunnable(() -> {
                AerospikeReactiveTransaction transaction = getTransaction(status);
                transaction.abortTransaction();
            })
            .onErrorMap(e -> new TransactionSystemException("Could not abort transaction", e))
            .then();
    }

    @Override
    protected Mono<Object> doSuspend(TransactionSynchronizationManager synchronizationManager, Object transaction)
        throws TransactionException {
        return Mono.fromSupplier(() -> {
            AerospikeReactiveTransaction aerospikeTransaction = toAerospikeTransaction(transaction);
            aerospikeTransaction.setResourceHolder(null);

            return synchronizationManager.unbindResource(client);
        }).onErrorMap(e -> new TransactionSystemException("Could not suspend transaction", e));
    }

    @Override
    protected Mono<Void> doResume(TransactionSynchronizationManager synchronizationManager,
                                  @Nullable Object transaction,
                                  Object suspendedResources) {
        return Mono.fromRunnable(() -> synchronizationManager.bindResource(client, suspendedResources))
            .onErrorMap(e -> new TransactionSystemException("Could not resume transaction", e))
            .then();
    }

    @Override
    protected Mono<Void> doSetRollbackOnly(TransactionSynchronizationManager synchronizationManager,
                                           GenericReactiveTransaction status) throws TransactionException {
        return Mono.fromRunnable(() -> {
                AerospikeReactiveTransaction transaction = toAerospikeTransaction(status);
                transaction.getRequiredResourceHolder().setRollbackOnly();
            })
            .onErrorMap(e -> new TransactionSystemException("Could not resume transaction", e))
            .then();
    }

    @Override
    protected Mono<Void> doCleanupAfterCompletion(TransactionSynchronizationManager synchronizationManager,
                                                  Object transaction) {
        return Mono.fromRunnable(() -> {
                AerospikeReactiveTransaction aerospikeTransaction = toAerospikeTransaction(transaction);

                // Remove the value (resource holder) from the thread.
                synchronizationManager.unbindResource(client);
                aerospikeTransaction.getRequiredResourceHolder().clear();
            })
            .onErrorMap(e -> new TransactionSystemException("Could not resume transaction", e))
            .then();
    }
}
