package org.springframework.data.aerospike.transaction.reactive;

import com.aerospike.client.Tran;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.Getter;
import org.springframework.transaction.support.ResourceHolderSupport;

@Getter
public class AerospikeReactiveTransactionResourceHolder extends ResourceHolderSupport {

    private final Tran transaction;
    private final IAerospikeReactorClient client;

    public AerospikeReactiveTransactionResourceHolder(IAerospikeReactorClient client) {
        this.client = client;
        this.transaction = new Tran();
    }

    public boolean hasTransaction() {
        return transaction != null;
    }
}
