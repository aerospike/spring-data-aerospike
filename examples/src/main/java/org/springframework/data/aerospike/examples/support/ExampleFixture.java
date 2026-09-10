package org.springframework.data.aerospike.examples.support;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Host;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.mapping.Document;

import java.util.Arrays;
import java.util.List;

import static com.aerospike.client.ResultCode.INDEX_NOTFOUND;

public interface ExampleFixture {

    default void beforeContextRefresh(Args args) {
    }

    default void setup(ConfigurableApplicationContext context) {
    }

    default void verify(ConfigurableApplicationContext context) {
    }

    default void cleanup(ConfigurableApplicationContext context) {
    }

    static ExampleFixture none() {
        return new ExampleFixture() {
        };
    }

    static ExampleFixture cleanSet(Class<?> entityClass) {
        return new CleanupFixture(entityClass, List.of(), false);
    }

    static ExampleFixture cleanSetAndIndexes(Class<?> entityClass, String... indexNames) {
        return new CleanupFixture(entityClass, Arrays.asList(indexNames), true, false);
    }

    static ExampleFixture cleanSetAndDropIndexesBeforeContextRefresh(Class<?> entityClass, String... indexNames) {
        return new CleanupFixture(entityClass, Arrays.asList(indexNames), false, true, List.of());
    }

    static DirectIndexDefinition index(String indexName, String binName, IndexType indexType) {
        return new DirectIndexDefinition(indexName, binName, indexType);
    }

    static ExampleFixture cleanSetAndCreateIndexBeforeContextRefresh(Class<?> entityClass, String indexName,
                                                                    String binName, IndexType indexType) {
        return cleanSetAndCreateIndexesBeforeContextRefresh(entityClass, index(indexName, binName, indexType));
    }

    static ExampleFixture cleanSetAndCreateIndexesBeforeContextRefresh(Class<?> entityClass,
                                                                       DirectIndexDefinition... indexes) {
        List<DirectIndexDefinition> indexDefinitions = Arrays.asList(indexes);
        List<String> indexNames = indexDefinitions.stream()
            .map(DirectIndexDefinition::indexName)
            .toList();
        return new CleanupFixture(entityClass, indexNames, false, true, indexDefinitions);
    }

    class CleanupFixture implements ExampleFixture {

        private final Class<?> entityClass;
        private final List<String> indexNames;
        private final boolean dropIndexesInSetup;
        private final boolean dropIndexesBeforeContextRefresh;
        private final List<DirectIndexDefinition> indexesToCreateBeforeContextRefresh;

        CleanupFixture(Class<?> entityClass, List<String> indexNames, boolean dropIndexesInSetup,
                       boolean dropIndexesBeforeContextRefresh) {
            this(entityClass, indexNames, dropIndexesInSetup, dropIndexesBeforeContextRefresh, List.of());
        }

        CleanupFixture(Class<?> entityClass, List<String> indexNames, boolean dropIndexesInSetup,
                       boolean dropIndexesBeforeContextRefresh,
                       List<DirectIndexDefinition> indexesToCreateBeforeContextRefresh) {
            this.entityClass = entityClass;
            this.indexNames = List.copyOf(indexNames);
            this.dropIndexesInSetup = dropIndexesInSetup;
            this.dropIndexesBeforeContextRefresh = dropIndexesBeforeContextRefresh;
            this.indexesToCreateBeforeContextRefresh = List.copyOf(indexesToCreateBeforeContextRefresh);
        }

        CleanupFixture(Class<?> entityClass, List<String> indexNames, boolean dropIndexesInSetup) {
            this(entityClass, indexNames, dropIndexesInSetup, false);
        }

        @Override
        public void beforeContextRefresh(Args args) {
            if ((dropIndexesBeforeContextRefresh && !indexNames.isEmpty())
                || !indexesToCreateBeforeContextRefresh.isEmpty()) {
                cleanupIndexes(args);
            }
        }

        @Override
        public void setup(ConfigurableApplicationContext context) {
            cleanupRecords(context);
            if (dropIndexesInSetup) {
                cleanupIndexes(context);
            }
        }

        @Override
        public void cleanup(ConfigurableApplicationContext context) {
            cleanupRecords(context);
            cleanupIndexes(context);
        }

        private void cleanupRecords(ConfigurableApplicationContext context) {
            AerospikeTemplate blockingTemplate = context.getBeanProvider(AerospikeTemplate.class).getIfAvailable();
            if (blockingTemplate != null) {
                blockingTemplate.deleteAll(entityClass);
                return;
            }

            ReactiveAerospikeTemplate reactiveTemplate =
                context.getBeanProvider(ReactiveAerospikeTemplate.class).getIfAvailable();
            if (reactiveTemplate != null) {
                reactiveTemplate.deleteAll(entityClass).block();
            }
        }

        private void cleanupIndexes(Args args) {
            ClientPolicy clientPolicy = new ClientPolicy();
            clientPolicy.failIfNotConnected = true;

            AerospikeClient client = new AerospikeClient(clientPolicy, Host.parseHosts(args.hosts(), 3000));
            try {
                indexNames.forEach(indexName -> dropDirectIndex(client, args.namespace(), indexName));
                indexesToCreateBeforeContextRefresh
                    .forEach(indexDefinition -> createDirectIndex(client, args.namespace(), indexDefinition));
            } finally {
                client.close();
            }
        }

        private void dropDirectIndex(AerospikeClient client, String namespace, String indexName) {
            try {
                IndexTask task = client.dropIndex(null, namespace, setName(), indexName);
                if (task != null) {
                    task.waitTillComplete();
                }
            } catch (AerospikeException ex) {
                if (ex.getResultCode() != INDEX_NOTFOUND) {
                    throw ex;
                }
            }
        }

        private void createDirectIndex(AerospikeClient client, String namespace, DirectIndexDefinition index) {
            IndexTask task = client.createIndex(null, namespace, setName(), index.indexName(), index.binName(),
                index.indexType());
            if (task != null) {
                task.waitTillComplete();
            }
        }

        private String setName() {
            Document document = entityClass.getAnnotation(Document.class);
            if (document != null && !document.collection().isBlank()) {
                return document.collection();
            }
            return entityClass.getSimpleName();
        }

        private void cleanupIndexes(ConfigurableApplicationContext context) {
            AerospikeTemplate blockingTemplate = context.getBeanProvider(AerospikeTemplate.class).getIfAvailable();
            if (blockingTemplate != null) {
                indexNames.forEach(indexName -> dropBlockingIndex(blockingTemplate, indexName));
                return;
            }

            ReactiveAerospikeTemplate reactiveTemplate =
                context.getBeanProvider(ReactiveAerospikeTemplate.class).getIfAvailable();
            if (reactiveTemplate != null) {
                indexNames.forEach(indexName -> dropReactiveIndex(reactiveTemplate, indexName));
            }
        }

        private void dropBlockingIndex(AerospikeTemplate template, String indexName) {
            try {
                template.deleteIndex(entityClass, indexName);
            } catch (RuntimeException ignored) {
                // Cleanup should not fail a scenario because a previous run already removed the index
            }
        }

        private void dropReactiveIndex(ReactiveAerospikeTemplate template, String indexName) {
            try {
                template.deleteIndex(entityClass, indexName).block();
            } catch (RuntimeException ignored) {
                // Cleanup should not fail a scenario because a previous run already removed the index
            }
        }
    }

    record DirectIndexDefinition(String indexName, String binName, IndexType indexType) {
    }
}
