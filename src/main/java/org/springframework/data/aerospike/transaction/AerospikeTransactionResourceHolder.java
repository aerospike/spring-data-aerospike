package org.springframework.data.aerospike.transaction;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Tran;
import lombok.Getter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.ResourceHolderSupport;

@Getter
public class AerospikeTransactionResourceHolder extends ResourceHolderSupport {

    private final Tran transaction;
    private final IAerospikeClient client;

    public AerospikeTransactionResourceHolder(IAerospikeClient client) {
        this.client = client;
        this.transaction = new Tran();
    }

    public boolean hasTransaction() {
        return transaction != null;
    }

    void setTimeoutIfNotDefault(int seconds) {
        if (seconds != TransactionDefinition.TIMEOUT_DEFAULT) {
            setTimeoutInSeconds(seconds);
        }
    }
}
