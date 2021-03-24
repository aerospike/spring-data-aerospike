package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.BlockingTestConfig;
import org.springframework.data.aerospike.config.CommonTestConfig;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.cache.IndexRefresher;

@SpringBootTest(
        classes = {BlockingTestConfig.class, CommonTestConfig.class},
        properties = {
                "expirationProperty: 1",
                "setSuffix: service1",
                "indexSuffix: index1"
        }
)
public abstract class BaseBlockingIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected AerospikeTemplate template;
    @Autowired
    protected AerospikeClient client;
    @Autowired
    protected QueryEngine queryEngine;
    @Autowired
    protected IndexesCache indexesCache;
    @Autowired
    protected IndexRefresher indexRefresher;

}