package org.springframework.data.aerospike.query.reactive;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.aerospike.BaseReactiveIntegrationTests;
import org.springframework.data.aerospike.query.QueryEngineTestDataPopulator;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import reactor.core.publisher.Mono;

public abstract class BaseReactiveQueryEngineTests extends BaseReactiveIntegrationTests {

	@Autowired
	ReactorQueryEngine queryEngine;
	@Autowired
	QueryEngineTestDataPopulator queryEngineTestDataPopulator;
	@Autowired
	ReactorIndexRefresher indexRefresher;

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
		reactorClient.dropIndex(null, namespace, setName, indexName)
				.onErrorResume(throwable -> throwable instanceof AerospikeException
								&& ((AerospikeException) throwable).getResultCode() == ResultCode.INDEX_NOTFOUND,
						throwable -> Mono.empty())
				.then(indexRefresher.refreshIndexes())
				.block();
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType) {
		tryCreateIndex(namespace, setName, indexName, binName, indexType, IndexCollectionType.DEFAULT);
	}

	protected void tryCreateIndex(String namespace, String setName, String indexName, String binName, IndexType indexType,
								  IndexCollectionType collectionType) {
		reactorClient.createIndex(null, namespace, setName, indexName, binName, indexType, collectionType)
				.onErrorResume(throwable -> throwable instanceof AerospikeException
								&& ((AerospikeException) throwable).getResultCode() == ResultCode.INDEX_ALREADY_EXISTS,
						throwable -> Mono.empty())
				.then(indexRefresher.refreshIndexes())
				.block();
	}
}
