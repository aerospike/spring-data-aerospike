package org.springframework.data.aerospike.transaction;

import com.aerospike.client.IAerospikeClient;
import lombok.Getter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

@Getter
public class AerospikeTransactionManager extends AbstractPlatformTransactionManager {

    private final IAerospikeClient client;

    /**
     * Create a new instance of {@link AerospikeTransactionManager}
     *
     */
    public AerospikeTransactionManager(IAerospikeClient client) {
        this.client = client;
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) throws TransactionException {
        return toTransaction(transaction).hasResourceHolder();
    }

    private static AerospikeTransaction toTransaction(Object transaction) {

        Assert.isInstanceOf(AerospikeTransaction.class, transaction,
            () -> String.format("Expected to find instance of %s but instead found %s", AerospikeTransaction.class,
                transaction.getClass()));

        return (AerospikeTransaction) transaction;
    }

    private static AerospikeTransaction getTransaction(DefaultTransactionStatus status) {

        Assert.isInstanceOf(AerospikeTransaction.class, status.getTransaction(),
            () -> String.format("Expected to find instance of %s but instead found %s", AerospikeTransaction.class,
                status.getTransaction().getClass()));

        return (AerospikeTransaction) status.getTransaction();
    }

    @Override
    protected Object doGetTransaction() throws TransactionException {

        AerospikeTransactionResourceHolder resourceHolder = (AerospikeTransactionResourceHolder) TransactionSynchronizationManager
            .getResource(getClient());
        return new AerospikeTransaction(resourceHolder);
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) throws TransactionException {
        // get transaction from the transaction object
        AerospikeTransaction aerospikeTransaction = toTransaction(transaction);
        // create new resourceHolder with a new Tran
        AerospikeTransactionResourceHolder resourceHolder = createResourceHolder(definition, client);
        // associate resourceHolder with the transaction
        aerospikeTransaction.setResourceHolder(resourceHolder);

        resourceHolder.setSynchronizedWithTransaction(true);
        // bind the resource to the current thread (get ThreadLocal map or set if not found, set value)
        TransactionSynchronizationManager.bindResource(client, resourceHolder);
    }

    private AerospikeTransactionResourceHolder createResourceHolder(TransactionDefinition definition, IAerospikeClient client) {
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
}
