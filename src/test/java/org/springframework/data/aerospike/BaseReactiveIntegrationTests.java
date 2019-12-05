package org.springframework.data.aerospike;

import com.aerospike.client.reactor.AerospikeReactorClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;

import java.io.Serializable;

public abstract class BaseReactiveIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;
    @Autowired
    protected AerospikeReactorClient reactorClient;

    protected <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }

}