package org.springframework.data.aerospike.transaction;

import org.springframework.lang.Nullable;

public class AerospikeTransaction {

    private @Nullable AerospikeTransactionResourceHolder resourceHolder;

    AerospikeTransaction(@Nullable AerospikeTransactionResourceHolder resourceHolder) {
        this.resourceHolder = resourceHolder;
    }

    /**
     * @return {@literal true} if {@link AerospikeTransactionResourceHolder} is set
     */
    final boolean hasResourceHolder() {
        return resourceHolder != null;
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
        if (!resourceHolder.hasTransaction()) throw new IllegalStateException("Error: expecting transaction to exist");
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
}
