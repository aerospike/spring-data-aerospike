package org.springframework.data.aerospike.transaction.reactive;

import org.springframework.data.aerospike.transaction.sync.AerospikeTransactionResourceHolder;
import org.springframework.lang.Nullable;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.util.Assert;

public class AerospikeReactiveTransaction implements SmartTransactionObject {

    private @Nullable AerospikeReactiveTransactionResourceHolder resourceHolder;

    AerospikeReactiveTransaction(@Nullable AerospikeReactiveTransactionResourceHolder resourceHolder) {
        this.resourceHolder = resourceHolder;
    }

    /**
     * @return {@literal true} if {@link AerospikeTransactionResourceHolder} is set
     */
    final boolean hasResourceHolder() {
        return resourceHolder != null;
    }

    AerospikeReactiveTransactionResourceHolder getRequiredResourceHolder() {
        Assert.state(resourceHolder != null, "Reactive resourceHolder is required to be not null");
        return resourceHolder;
    }

    /**
     * Set corresponding {@link AerospikeTransactionResourceHolder}
     *
     * @param resourceHolder can be {@literal null}.
     */
    void setResourceHolder(@Nullable AerospikeReactiveTransactionResourceHolder resourceHolder) {
        this.resourceHolder = resourceHolder;
    }

    private void failIfNoTransaction() {
        if (!resourceHolder.hasTransaction()) throw new IllegalStateException("Error: expecting transaction to exist");
    }

    /**
     * Commit the transaction.
     */
    public void commitTransaction() {
        failIfNoTransaction();
        resourceHolder.getClient().getAerospikeClient().commit(resourceHolder.getTransaction());
    }

    /**
     * Rollback (abort) the transaction.
     */
    public void abortTransaction() {
        failIfNoTransaction();
        resourceHolder.getClient().getAerospikeClient().abort(resourceHolder.getTransaction());
    }

    @Override
    public boolean isRollbackOnly() {
        return this.resourceHolder != null && this.resourceHolder.isRollbackOnly();
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("Currently flush() is not supported for a reactive transaction");
    }
}
