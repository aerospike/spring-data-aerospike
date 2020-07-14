package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;

import java.util.function.Supplier;

public class IndexUtils {

	public static void dropIndex(AerospikeClient client, String namespace, String setName, String indexName) {
		ignoreErrorAndWait(ResultCode.INDEX_NOTFOUND, () -> client.dropIndex(null, namespace, setName, indexName));
	}

	public static void createIndex(AerospikeClient client, String namespace, String setName, String indexName, String binName, IndexType indexType) {
		ignoreErrorAndWait(ResultCode.INDEX_ALREADY_EXISTS, () -> client.createIndex(null, namespace, setName, indexName, binName, indexType));
	}

	public static void createIndex(AerospikeClient client, String namespace, String setName, String indexName, String binName, IndexType indexType, IndexCollectionType collectionType) {
		ignoreErrorAndWait(ResultCode.INDEX_ALREADY_EXISTS, () -> client.createIndex(null, namespace, setName, indexName, binName, indexType, collectionType));
	}

	public static boolean indexExists(AerospikeClient client, String namespace, String indexName) {
		Node[] nodes = client.getNodes();
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}
		Node node = nodes[0];
		String response = Info.request(node, "sindex/" + namespace + '/' + indexName);
		return !response.startsWith("FAIL:201");
	}

	private static void ignoreErrorAndWait(int errorCodeToSkip, Supplier<IndexTask> supplier) {
		try {
			IndexTask task = supplier.get();
			if (task == null) {
				throw new IllegalStateException("task can not be null");
			}
			task.waitTillComplete();
		} catch (AerospikeException e) {
			if (e.getResultCode() != errorCodeToSkip) {
				throw e;
			}
		}
	}
}
