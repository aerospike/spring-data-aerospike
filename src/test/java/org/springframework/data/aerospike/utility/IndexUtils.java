package org.springframework.data.aerospike.utility;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cdt.CTX;
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

    static void dropIndex(IAerospikeClient client, ServerVersionUtils serverVersionUtils, String namespace,
                          String setName, String indexName) {
        if (serverVersionUtils.isDropCreateBehaviorUpdated()) {
            waitTillComplete(() -> client.dropIndex(null, namespace, setName, indexName));
        } else {
            // ignoring ResultCode.INDEX_NOTFOUND for Aerospike Server prior to ver. 6.1.0.1
            ignoreErrorAndWait(ResultCode.INDEX_NOTFOUND, () -> client.dropIndex(null, namespace, setName, indexName));
        }
    }

    static void createIndex(IAerospikeClient client, ServerVersionUtils serverVersionUtils, String namespace,
                            String setName, String indexName, String binName, IndexType indexType,
                            IndexCollectionType collectionType, CTX[] ctx) {
        if (serverVersionUtils.isDropCreateBehaviorUpdated()) {
            waitTillComplete(() -> client.createIndex(null, namespace, setName, indexName, binName, indexType,
                collectionType, ctx));
        } else {
            // ignoring ResultCode.INDEX_ALREADY_EXISTS for Aerospike Server prior to ver. 6.1.0.1
            ignoreErrorAndWait(ResultCode.INDEX_ALREADY_EXISTS, () -> client.createIndex(null, namespace, setName,
                indexName, binName, indexType, collectionType, ctx));
        }
    }

    public static List<Index> getIndexes(IAerospikeClient client, String namespace, IndexInfoParser indexInfoParser) {
        Node node = client.getCluster().getRandomNode();
        String response = Info.request(node, "sindex-list:ns=" + namespace + ";b64=true");
        return Arrays.stream(response.split(";"))
            .map(indexInfoParser::parse)
            .collect(Collectors.toList());
    }

    /**
     * @deprecated since Aerospike Server ver. 6.1.0.1. Use
     * {@link org.springframework.data.aerospike.core.AerospikeTemplate#indexExists(String)}
     */
    public static boolean indexExists(IAerospikeClient client, String namespace, String indexName) {
        Node node = client.getCluster().getRandomNode();
        String response = Info.request(node, "sindex/" + namespace + '/' + indexName);
        return !response.startsWith("FAIL:201");
    }

    private static void waitTillComplete(Supplier<IndexTask> supplier) {
        IndexTask task = supplier.get();
        if (task == null) {
            throw new IllegalStateException("Task can not be null");
        }
        task.waitTillComplete();
    }

    private static void ignoreErrorAndWait(int errorCodeToSkip, Supplier<IndexTask> supplier) {
        try {
            IndexTask task = supplier.get();
            if (task == null) {
                throw new IllegalStateException("Task can not be null");
            }
            task.waitTillComplete();
        } catch (AerospikeException e) {
            if (e.getResultCode() != errorCodeToSkip) {
                throw e;
            }
        }
    }
}
