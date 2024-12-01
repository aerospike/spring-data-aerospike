package org.springframework.data.aerospike.transaction.sync;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

public class TestTransactionSynchronization implements TransactionSynchronization {

    private final Runnable afterCompletion;

    public TestTransactionSynchronization(Runnable afterCompletion) {
        this.afterCompletion = afterCompletion;
    }

    @Override
    public void afterCompletion(int status) {
        // after transaction completion (commit/rollback)
        if (afterCompletion != null) {
            afterCompletion.run();
        }
    }

    public void register() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(this);
        }
    }
}
