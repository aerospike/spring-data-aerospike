package org.springframework.data.aerospike.query.blocking;

import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.query.QueryEngineTestDataPopulator;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.util.AwaitilityUtils;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseQueryEngineTests extends BaseBlockingIntegrationTests {

    @Autowired
    QueryEngineTestDataPopulator queryEngineTestDataPopulator;

    @BeforeEach
    public void setUp() {
        queryEngineTestDataPopulator.setupAllData();
    }

    protected void withIndex(String namespace, String setName, String indexName, String binName, IndexType indexType,
                             Runnable runnable) {
        tryCreateIndex(namespace, setName, indexName, binName, indexType);
        try {
            runnable.run();
        } finally {
            tryDropIndex(setName, indexName);
        }
    }

    protected void tryDropIndex(String setName, String indexName) {
        additionalAerospikeTestOperations.dropIndex(setName, indexName);
    }

    protected void tryCreateIndex(String namespace, String setName, String indexName, String binName,
                                  IndexType indexType) {
        AwaitilityUtils.awaitTenSecondsUntil(() -> {
            additionalAerospikeTestOperations.createIndex(namespace, setName, indexName, binName, indexType);
            IndexKey indexKey = new IndexKey(namespace, setName, binName, indexType, null);
            Optional<Index> index = indexesCache.getIndex(indexKey);
            assertThat(index).as("Index for: " + indexKey + " not created").isPresent();
        });
    }
}
