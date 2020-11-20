package org.springframework.data.aerospike.query;

import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.AwaitilityUtils;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.IndexUtils;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseQueryEngineTests extends BaseBlockingIntegrationTests {

	@Autowired
	QueryEngineTestDataPopulator queryEngineTestDataPopulator;

	@BeforeEach
	public void setUp() {
		queryEngineTestDataPopulator.setupAllData();
	}

	protected void withIndex(String namespace, String setName, String indexName, String binName, IndexType indexType, Runnable runnable) {
		tryCreateIndex(namespace, setName, indexName, binName, indexType);
		try {
			runnable.run();
		} finally {
			tryDropIndex(namespace, setName, indexName);
		}
	}

	protected void tryDropIndex(String namespace, String setName, String indexName) {
		IndexUtils.dropIndex(client, namespace, setName, indexName);
		indexRefresher.refreshIndexes();
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType) {
		AwaitilityUtils.awaitTenSecondsUntil(() -> {
			IndexUtils.createIndex(client, namespace, setName, indexName, binName, indexType);
			indexRefresher.refreshIndexes();
			IndexKey indexKey = new IndexKey(namespace, setName, binName, indexType, IndexCollectionType.DEFAULT);
			Optional<Index> index = indexesCache.getIndex(indexKey);
			assertThat(index).as("Index for: " + indexKey + " not created").isPresent();
		});
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType,
								  IndexCollectionType collectionType) {
		IndexUtils.createIndex(client, namespace, setName, indexName, binName, indexType, collectionType);
		indexRefresher.refreshIndexes();
	}

}
