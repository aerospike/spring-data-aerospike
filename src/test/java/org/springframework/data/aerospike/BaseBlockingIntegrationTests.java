package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.core.AerospikeTemplate;

public abstract class BaseBlockingIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected AerospikeTemplate template;
    @Autowired
    protected AerospikeClient client;

}