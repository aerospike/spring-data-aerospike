package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.query.IndexType;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.InvalidDataAccessResourceUsageException;
import org.springframework.data.aerospike.config.TestConfig;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.Person;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.atomic.AtomicLong;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(
        classes = TestConfig.class,
        properties = {
                "expirationProperty: 1",
        }
)
public abstract class BaseIntegrationTests {

    private static AtomicLong counter = new AtomicLong();

    @Value("${embedded.aerospike.namespace}")
    protected String namespace;

    @Autowired
    protected AerospikeTemplate template;
    @Autowired
    protected AerospikeClient client;

    protected String getNameSpace() {
        return namespace;
    }

    protected static String nextId() {
        return "as-" + counter.incrementAndGet();
    }

    protected void cleanDb() {
        template.delete(Person.class);
        template.delete(SampleClasses.VersionedClass.class);
    }

    protected <T> void createIndexIfNotExists(Class<T> domainType, String indexName, String binName, IndexType indexType) {
        try {
            template.createIndex(domainType, indexName, binName, indexType);
        } catch (InvalidDataAccessResourceUsageException e) {
            // ignore: index already exists
        }
    }

}
