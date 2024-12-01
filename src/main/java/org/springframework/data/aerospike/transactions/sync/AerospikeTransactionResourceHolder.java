package org.springframework.data.aerospike.transactions.sync;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Txn;
import lombok.Getter;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.ResourceHolderSupport;

@Getter
public class AerospikeTransactionResourceHolder extends ResourceHolderSupport {

    private final Txn transaction;
    private final IAerospikeClient client;

    public AerospikeTransactionResourceHolder(IAerospikeClient client) {
        this.client = client;
        this.transaction = new Txn();
    }

    void setTimeoutIfNotDefault(int seconds) {
        if (seconds != TransactionDefinition.TIMEOUT_DEFAULT) {
            transaction.setTimeout(seconds);
            setTimeoutInSeconds(seconds);
        }
    }
}
