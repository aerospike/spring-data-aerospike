package org.springframework.data.aerospike.transactions.sync;

import org.springframework.lang.Nullable;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationUtils;
import org.springframework.util.Assert;

public class AerospikeTransaction implements SmartTransactionObject {

    @Nullable
    private AerospikeTransactionResourceHolder resourceHolder;

    AerospikeTransaction(@Nullable AerospikeTransactionResourceHolder resourceHolder) {
        this.resourceHolder = resourceHolder;
    }

    /**
     * @return {@literal true} if {@link AerospikeTransactionResourceHolder} is set
     */
    final boolean hasResourceHolder() {
        return resourceHolder != null;
    }

    AerospikeTransactionResourceHolder getResourceHolderOrFail() {
        Assert.state(hasResourceHolder(), "ResourceHolder is required to be not null");
        return resourceHolder;
    }

    /**
     * Set corresponding {@link AerospikeTransactionResourceHolder}
     *
     * @param resourceHolder can be {@literal null}.
     */
    void setResourceHolder(@Nullable AerospikeTransactionResourceHolder resourceHolder) {
        this.resourceHolder = resourceHolder;
    }

    private void failIfNoTransaction() {
        if (!hasResourceHolder()) {
            throw new IllegalStateException("Error: expecting transaction to exist");
        }
    }

    /**
     * Commit the transaction.
     */
    public void commitTransaction() {
        failIfNoTransaction();
        resourceHolder.getClient().commit(resourceHolder.getTransaction());
    }

    /**
     * Rollback (abort) the transaction.
     */
    public void abortTransaction() {
        failIfNoTransaction();
        resourceHolder.getClient().abort(resourceHolder.getTransaction());
    }

    @Override
    public boolean isRollbackOnly() {
        return hasResourceHolder() && this.resourceHolder.isRollbackOnly();
    }

    @Override
    public void flush() {
        TransactionSynchronizationUtils.triggerFlush();
    }
}
