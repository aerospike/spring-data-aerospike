package org.springframework.data.aerospike.transactions.reactive;

import com.aerospike.client.Txn;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.Getter;
import org.springframework.transaction.support.ResourceHolderSupport;

@Getter
public class AerospikeReactiveTransactionResourceHolder extends ResourceHolderSupport {

    private final Txn transaction;
    private final IAerospikeReactorClient client;

    public AerospikeReactiveTransactionResourceHolder(IAerospikeReactorClient client) {
        this.client = client;
        this.transaction = new Txn();
    }
}
