package org.springframework.data.aerospike;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
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
        waitTillComplete(() -> client.dropIndex(null, namespace, setName, indexName));
    }

    public static void createIndex(IAerospikeClient client, String namespace, String setName, String indexName,
                                   String binName, IndexType indexType) {
        waitTillComplete(() -> client.createIndex(null, namespace, setName, indexName, binName, indexType));
    }

    public static void createIndex(IAerospikeClient client, String namespace, String setName, String indexName,
                                   String binName, IndexType indexType, IndexCollectionType collectionType) {
        waitTillComplete(() -> client.createIndex(null, namespace, setName, indexName, binName, indexType,
            collectionType));
    }

    public static List<Index> getIndexes(IAerospikeClient client, String namespace, IndexInfoParser indexInfoParser) {
        Node node = getNode(client);
        String response = Info.request(node, "sindex-list:ns=" + namespace + ";b64=true");
        return Arrays.stream(response.split(";"))
            .map(indexInfoParser::parse)
            .collect(Collectors.toList());
    }

    public static boolean indexExists(IAerospikeClient client, String namespace, String indexName) {
        Node node = getNode(client);
        String response = Info.request(node, "sindex/" + namespace + '/' + indexName);
        return !response.startsWith("FAIL:201");
    }

    private static void waitTillComplete(Supplier<IndexTask> supplier) {
        IndexTask task = supplier.get();
        if (task == null) {
            throw new IllegalStateException("task can not be null");
        }
        task.waitTillComplete();
    }

    private static Node getNode(IAerospikeClient client) {
        Node[] nodes = client.getNodes();
        if (nodes.length == 0) {
            throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
        }
        return nodes[0];
    }
}
