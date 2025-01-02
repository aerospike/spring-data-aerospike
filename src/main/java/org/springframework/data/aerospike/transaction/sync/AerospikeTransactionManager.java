package org.springframework.data.aerospike.transaction.sync;

import com.aerospike.client.IAerospikeClient;
import lombok.Getter;
import org.springframework.lang.Nullable;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * A {@link org.springframework.transaction.PlatformTransactionManager} implementation for managing transactions
 */
@Getter
public class AerospikeTransactionManager extends AbstractPlatformTransactionManager {

    private final IAerospikeClient client;

    /**
     * Create a new instance of {@link AerospikeTransactionManager}
     */
    public AerospikeTransactionManager(IAerospikeClient client) {
        this.client = client;
    }

    private static AerospikeTransaction toAerospikeTransaction(Object transaction) {
        Assert.isInstanceOf(AerospikeTransaction.class, transaction,
            () -> String.format("Expected to find instance of %s but instead found %s", AerospikeTransaction.class,
                transaction.getClass()));

        return (AerospikeTransaction) transaction;
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        return toAerospikeTransaction(transaction).hasResourceHolder();
    }

    private static AerospikeTransaction getTransaction(DefaultTransactionStatus status) {
        Assert.isInstanceOf(AerospikeTransaction.class, status.getTransaction(),
            () -> String.format("Expected to find instance of %s but instead found %s", AerospikeTransaction.class,
                status.getTransaction().getClass()));

        return (AerospikeTransaction) status.getTransaction();
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {
        AerospikeTransactionResourceHolder resourceHolder =
            (AerospikeTransactionResourceHolder) TransactionSynchronizationManager.getResource(getClient());
        return new AerospikeTransaction(resourceHolder);
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        AerospikeTransaction aerospikeTransaction;
        AerospikeTransactionResourceHolder resourceHolder;
        try {
            // get transaction from the transaction object
            aerospikeTransaction = toAerospikeTransaction(transaction);
            // create new resourceHolder with a new Tran, de facto start transaction
            resourceHolder = createResourceHolder(definition, client);
        } catch (Exception e) {
            throw new TransactionSystemException("Could not start transaction", e);
        }

        // associate resourceHolder with the transaction
        aerospikeTransaction.setResourceHolder(resourceHolder);

        resourceHolder.setSynchronizedWithTransaction(true);
        // bind the resource to the current thread (get ThreadLocal map or set if not found, set value)
        TransactionSynchronizationManager.bindResource(client, resourceHolder);
    }

    private AerospikeTransactionResourceHolder createResourceHolder(TransactionDefinition definition,
                                                                    IAerospikeClient client) {
        AerospikeTransactionResourceHolder resourceHolder = new AerospikeTransactionResourceHolder(client);
        resourceHolder.setTimeoutIfNotDefault(determineTimeout(definition));
        return resourceHolder;
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) throws TransactionException {
        AerospikeTransaction transaction = getTransaction(status); // get transaction with associated resourceHolder
        transaction.commitTransaction();
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) throws TransactionException {
        AerospikeTransaction transaction = getTransaction(status); // get transaction with associated resourceHolder
        transaction.abortTransaction();
    }

    @Override
    protected Object doSuspend(Object transaction) throws TransactionException {
        try {
            AerospikeTransaction aerospikeTransaction = toAerospikeTransaction(transaction);
            aerospikeTransaction.setResourceHolder(null);
            return TransactionSynchronizationManager.unbindResource(client);
        } catch (Exception e) {
            throw new TransactionSystemException("Could not suspend transaction", e);
        }
    }

    @Override
    protected void doResume(@Nullable Object transaction, Object suspendedResources) throws TransactionException {
        try {
            TransactionSynchronizationManager.bindResource(client, suspendedResources);
        } catch (Exception e) {
            throw new TransactionSystemException("Could not resume transaction", e);
        }
    }

    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) throws TransactionException {
        try {
            AerospikeTransaction transaction = getTransaction(status);
            transaction.getResourceHolderOrFail().setRollbackOnly();
        } catch (Exception e) {
            throw new TransactionSystemException("Could not set transaction to rollback-only", e);
        }
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        AerospikeTransaction aerospikeTransaction = toAerospikeTransaction(transaction);

        // Remove the value (resource holder) from the thread
        TransactionSynchronizationManager.unbindResource(client);
        aerospikeTransaction.getResourceHolderOrFail().clear();
    }
}
