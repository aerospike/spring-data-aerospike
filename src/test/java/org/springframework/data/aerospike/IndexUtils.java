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

import java.lang.module.ModuleDescriptor;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IndexUtils {

    private static final ModuleDescriptor.Version SERVER_VERSION_6_1_0_1 = ModuleDescriptor.Version.parse("6.1.0.1");
    private static final ModuleDescriptor.Version SERVER_VERSION_6_3_0_0 = ModuleDescriptor.Version.parse("6.3.0.0");

    public static void dropIndex(IAerospikeClient client, String namespace, String setName, String indexName) {
        if (IndexUtils.isDropCreateBehaviorUpdated(client)) {
            waitTillComplete(() -> client.dropIndex(null, namespace, setName, indexName));
        } else {
            // ignoring ResultCode.INDEX_NOTFOUND for Aerospike Server prior to ver. 6.1.0.1
            ignoreErrorAndWait(ResultCode.INDEX_NOTFOUND, () -> client.dropIndex(null, namespace, setName, indexName));
        }
    }

    public static void createIndex(IAerospikeClient client, String namespace, String setName, String indexName,
                                   String binName, IndexType indexType) {
        if (IndexUtils.isDropCreateBehaviorUpdated(client)) {
            waitTillComplete(() -> client.createIndex(null, namespace, setName, indexName, binName, indexType));
        } else {
            // ignoring ResultCode.INDEX_ALREADY_EXISTS for Aerospike Server prior to ver. 6.1.0.1
            ignoreErrorAndWait(ResultCode.INDEX_ALREADY_EXISTS, () -> client.createIndex(null, namespace, setName,
                indexName, binName, indexType));
        }
    }

    public static void createIndex(IAerospikeClient client, String namespace, String setName, String indexName,
                                   String binName, IndexType indexType, IndexCollectionType collectionType) {
        if (IndexUtils.isDropCreateBehaviorUpdated(client)) {
            waitTillComplete(() -> client.createIndex(null, namespace, setName, indexName, binName, indexType,
                collectionType));
        } else {
            // ignoring ResultCode.INDEX_ALREADY_EXISTS for Aerospike Server prior to ver. 6.1.0.1
            ignoreErrorAndWait(ResultCode.INDEX_ALREADY_EXISTS, () -> client.createIndex(null, namespace, setName,
                indexName, binName, indexType, collectionType));
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

    public static String getServerVersion(IAerospikeClient client) {
        String versionString = Info.request(client.getCluster().getRandomNode(), "version");
        return versionString.substring(versionString.lastIndexOf(' ') + 1);
    }

    /**
     * Since Aerospike Server ver. 6.1.0.1 attempting to create a secondary index which already exists or to drop a
     * non-existing secondary index now returns success/OK instead of an error.
     */
    public static boolean isDropCreateBehaviorUpdated(IAerospikeClient client) {
        return ModuleDescriptor.Version.parse(IndexUtils.getServerVersion(client))
            .compareTo(SERVER_VERSION_6_1_0_1) >= 0;
    }

    /**
     * Since Aerospike Server ver. 6.3.0.0 find by POJO is supported.
     */
    public static boolean isFindByPojoSupported(IAerospikeClient client) {
        return ModuleDescriptor.Version.parse(IndexUtils.getServerVersion(client))
            .compareTo(SERVER_VERSION_6_3_0_0) >= 0;
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
