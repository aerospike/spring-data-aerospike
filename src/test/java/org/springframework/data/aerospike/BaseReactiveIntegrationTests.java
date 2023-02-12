package org.springframework.data.aerospike;

import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.aerospike.config.CommonTestConfig;
import org.springframework.data.aerospike.config.ReactiveTestConfig;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;

import java.io.Serializable;

@SpringBootTest(
        classes = {ReactiveTestConfig.class, CommonTestConfig.class},
        properties = {
                "expirationProperty: 1",
                "setSuffix: service1",
                "indexSuffix: index1"
        }
)
public abstract class BaseReactiveIntegrationTests extends BaseIntegrationTests {

    @Autowired
    protected ReactiveAerospikeTemplate reactiveTemplate;
    @Autowired
    protected IAerospikeReactorClient reactorClient;

    protected <T> T findById(Serializable id, Class<T> type) {
        return reactiveTemplate.findById(id, type).block();
    }
}