package org.springframework.data.aerospike;

import com.aerospike.client.*;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.model.Index;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IndexUtils {

	public static void dropIndex(IAerospikeClient client, String namespace, String setName, String indexName) {
		ignoreErrorAndWait(ResultCode.INDEX_NOTFOUND, () -> client.dropIndex(null, namespace, setName, indexName));
	}

	public static void createIndex(IAerospikeClient client, String namespace, String setName, String indexName, String binName, IndexType indexType) {
		ignoreErrorAndWait(ResultCode.INDEX_ALREADY_EXISTS, () -> client.createIndex(null, namespace, setName, indexName, binName, indexType));
	}

	public static void createIndex(IAerospikeClient client, String namespace, String setName, String indexName, String binName, IndexType indexType, IndexCollectionType collectionType) {
		ignoreErrorAndWait(ResultCode.INDEX_ALREADY_EXISTS, () -> client.createIndex(null, namespace, setName, indexName, binName, indexType, collectionType));
	}

	public static List<Index> getIndexes(IAerospikeClient client, String namespace, IndexInfoParser indexInfoParser) {
		Node node = getNode(client);
		String response = Info.request(node, "sindex/" + namespace);
		return Arrays.stream(response.split(";"))
				.map(indexInfoParser::parse)
				.collect(Collectors.toList());
	}

	public static boolean indexExists(IAerospikeClient client, String namespace, String indexName) {
		Node node = getNode(client);
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

	private static Node getNode(IAerospikeClient client) {
		Node[] nodes = client.getNodes();
		if (nodes.length == 0) {
			throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
		}
		return nodes[0];
	}
}
