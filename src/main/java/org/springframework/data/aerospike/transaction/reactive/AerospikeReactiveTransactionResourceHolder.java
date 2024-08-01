package org.springframework.data.aerospike.transaction.reactive;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Tran;
import lombok.Getter;
import org.springframework.transaction.support.ResourceHolderSupport;

@Getter
public class AerospikeReactiveTransactionResourceHolder extends ResourceHolderSupport {

    private final Tran transaction;
    private final IAerospikeClient client;

    public AerospikeReactiveTransactionResourceHolder(IAerospikeClient client) {
        this.client = client;
        this.transaction = new Tran();
    }

    public boolean hasTransaction() {
        return transaction != null;
    }
}
