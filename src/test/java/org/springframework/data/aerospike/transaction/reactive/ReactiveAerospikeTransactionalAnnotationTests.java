package org.springframework.data.aerospike.transaction.reactive;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public class ReactiveAerospikeTransactionalAnnotationTests extends BaseReactiveIntegrationTests {

    @Test
    @Transactional
    public void test1() {
        reactiveTemplate.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(100))).blockLast(); // TODO: test
    }

    @Test
    @Transactional
    public void test2() { // TODO: direct calls to client within a transaction
        WritePolicy wp = reactorClient.getWritePolicyDefault();
        wp.expiration = 1;
        // some specific configuration
        Key key = new Key("TEST", "testSet", "newKey1");
        reactorClient.put(wp, key, new Bin("bin1", "val1"));

        WritePolicy wp2 = reactorClient.getWritePolicyDefault();
        wp.durableDelete = true;
        // some specific configuration
        Key key2 = new Key("TEST", "testSet", "existingKey2");
        reactorClient.delete(wp2, key2);
    }
}
