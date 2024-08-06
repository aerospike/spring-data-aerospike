package org.springframework.data.aerospike.transaction.sync;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;
import org.junit.jupiter.api.Test;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public class AerospikeTransactionalAnnotationTests extends BaseBlockingIntegrationTests {

    @Test
    @Transactional
    public void test1() {
        template.insertAll(List.of(new SampleClasses.DocumentWithPrimitiveIntId(100))); // TODO: test
    }

    @Test
    @Transactional
    public void test2() { // TODO: direct calls to client within a transaction
        WritePolicy wp = client.copyWritePolicyDefault();
        wp.expiration = 1;
        // some specific configuration
        Key key = new Key("TEST", "testSet", "newKey1");
        client.put(wp, key, new Bin("bin1", "val1"));

        WritePolicy wp2 = client.copyWritePolicyDefault();
        wp.durableDelete = true;
        // some specific configuration
        Key key2 = new Key("TEST", "testSet", "existingKey2");
        client.delete(wp2, key2);
    }
}
