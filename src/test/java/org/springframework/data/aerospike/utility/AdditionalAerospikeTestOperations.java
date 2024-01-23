package org.springframework.data.aerospike.utility;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import org.awaitility.Awaitility;
import org.springframework.data.aerospike.core.WritePolicyBuilder;
import org.springframework.data.aerospike.index.IndexesCacheRefresher;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.sample.Customer;
import org.springframework.data.aerospike.sample.Person;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.client.Value.get;
import static com.aerospike.client.query.IndexCollectionType.DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

@RequiredArgsConstructor
public abstract class AdditionalAerospikeTestOperations {

    private final IndexInfoParser indexInfoParser;
    private final IAerospikeClient client;
    private final ServerVersionSupport serverVersionSupport;
    private final IndexesCacheRefresher indexesRefresher;
    private final GenericContainer<?> aerospike;

    public void assertScansForSet(String setName, Consumer<List<? extends ScanJob>> consumer) {
        List<ScanJob> jobs = getScans();
        List<ScanJob> jobsForSet = jobs.stream().filter(job -> job.set.equals(setName)).collect(Collectors.toList());
        assertThat(jobsForSet)
            .as("Scan jobs for set: " + setName)
            .satisfies(consumer);
    }

    public void assertNoScansForSet(String setName) {
        List<ScanJob> jobs = getScans();
        List<ScanJob> jobsForSet = jobs.stream().filter(job -> setName.equals(job.set)).toList();
        jobsForSet.forEach(job -> assertThat(job.getStatus()).isEqualTo("done(ok)"));
    }

    @SneakyThrows
    public List<ScanJob> getScans() {
        Container.ExecResult execResult = aerospike.execInContainer("asinfo", "-v", "scan-show");
        String stdout = execResult.getStdout();
        return getScanJobs(stdout);
    }

    private List<ScanJob> getScanJobs(String stdout) {
        if (stdout.isEmpty() || stdout.equals("\n")) {
            return Collections.emptyList();
        }
        String response = stdout.replaceAll("\n", "");
        return ResponseUtils.parseResponse(response, pairsMap -> ScanJob.builder()
            .module(pairsMap.get("module"))
            .set(pairsMap.get("set"))
            .udfFunction(pairsMap.get("udf-function"))
            .status(pairsMap.get("status"))
            .build());
    }

    @SneakyThrows
    public void killAllScans() {
        Container.ExecResult execResult = aerospike.execInContainer("asinfo", "-v", "scan-abort-all:");
        assertThat(execResult.getStdout())
            .as("Scan jobs killed")
            .contains("OK");
    }

    public void deleteAllAndVerify(Class<?>... entityClasses) {
        Arrays.asList(entityClasses).forEach(this::truncateSetOfEntityClass);
        Arrays.asList(entityClasses).forEach(this::awaitUntilSetIsEmpty);
    }

    public void deleteAllAndVerify(Class<?> entityClass, String setName) {
        truncateSet(setName);
        awaitUntilSetIsEmpty(entityClass, setName);
    }

    private void awaitUntilSetIsEmpty(Class<?> entityClass) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> isEntityClassSetEmpty(entityClass));
    }

    private void awaitUntilSetIsEmpty(Class<?> entityClass, String setName) {
        Awaitility.await()
            .atMost(Duration.ofSeconds(10))
            .until(() -> isSetEmpty(entityClass, setName));
    }

    public <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                                IndexType indexType) {
        createIndex(entityClass, indexName, binName, indexType, null, (CTX[]) null);
    }

    public <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                                IndexType indexType, IndexCollectionType indexCollectionType) {
        createIndex(entityClass, indexName, binName, indexType, indexCollectionType, (CTX[]) null);
    }

    public <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                                IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx) {
        createIndex(getNamespace(), getSetName(entityClass), indexName, binName, indexType,
            indexCollectionType, ctx);
    }

    public void createIndex(String setName, String indexName, String binName, IndexType indexType) {
        createIndex(getNamespace(), setName, indexName, binName, indexType, null, (CTX[]) null);
    }

    public void createIndex(String namespace, String setName, String indexName, String binName,
                            IndexType indexType) {
        createIndex(namespace, setName, indexName, binName, indexType, null, (CTX[]) null);
    }

    public void createIndex(String namespace, String setName, String indexName, String binName,
                            IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx) {
        IndexCollectionType collType = indexCollectionType == null ? DEFAULT : indexCollectionType;
        IndexUtils.createIndex(client, serverVersionSupport, namespace, setName, indexName, binName, indexType, collType,
            ctx);
        indexesRefresher.refreshIndexesCache();
    }

    public void createIndexes(Collection<Index> indexesToBeCreated) {
        indexesToBeCreated.forEach(index -> {
                IndexCollectionType collType = index.getIndexCollectionType() == null
                    ? DEFAULT : index.getIndexCollectionType();

                IndexUtils.createIndex(client, serverVersionSupport, getNamespace(), index.getSet(), index.getName(),
                    index.getBin(), index.getIndexType(), collType, index.getCtx());
            }
        );
        indexesRefresher.refreshIndexesCache();
    }

    public <T> void dropIndex(String setName, String indexName) {
        IndexUtils.dropIndex(client, serverVersionSupport, getNamespace(), setName, indexName);
        indexesRefresher.refreshIndexesCache();
    }

    public <T> void dropIndex(Class<T> entityClass, String indexName) {
        dropIndex(getSetName(entityClass), indexName);
    }

    public void dropIndexes(Collection<Index> indexesToBeDropped) {
        indexesToBeDropped.forEach(index -> {
                IndexUtils.dropIndex(client, serverVersionSupport, getNamespace(), index.getSet(), index.getName());
            }
        );
        indexesRefresher.refreshIndexesCache();
    }

    public <T> void deleteAll(AerospikeRepository<T, ?> repository, Collection<T> entities) {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            try {
                repository.deleteAll(entities);
            } catch (AerospikeException.BatchRecordArray ignored) {
                // KEY_NOT_FOUND ResultCode causes exception if there are no entities
            }
        } else {
            entities.forEach(repository::delete);
        }
    }

    public <T> void saveAll(AerospikeRepository<T, ?> repository, Collection<T> entities) {
        // batch write operations are supported starting with Server version 6.0+
        if (serverVersionSupport.isBatchWriteSupported()) {
            repository.saveAll(entities);
        } else {
            entities.forEach(repository::save);
        }
    }

    /**
     * @deprecated since Aerospike Server ver. 6.1.0.1. Use
     * {@link org.springframework.data.aerospike.core.AerospikeTemplate#indexExists(String)}
     */
    // Do not use this code in production!
    // This will not guarantee the correct answer from Aerospike Server for all cases.
    // Also, it requests index status only from one Aerospike node, which is OK for tests, and NOT OK for Production
    // cluster.
    @Deprecated
    public boolean indexExists(String indexName) {
        return IndexUtils.indexExists(client, getNamespace(), indexName);
    }

    public List<Index> getIndexes(String setName) {
        return getIndexes().stream()
            .filter(index -> index.getSet().equalsIgnoreCase(setName))
            .collect(Collectors.toList());
    }

    public List<Index> getIndexes() {
        return IndexUtils.getIndexes(client, getNamespace(), indexInfoParser);
    }

    public void addNewFieldToSavedDataInAerospike(Key key) {
        Record initial = client.get(new Policy(), key);
        Bin[] bins = Stream.concat(
            initial.bins.entrySet().stream().map(e -> new Bin(e.getKey(), get(e.getValue()))),
            Stream.of(new Bin("notPresent", "cats"))).toArray(Bin[]::new);

        WritePolicy policy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
            .recordExistsAction(RecordExistsAction.REPLACE)
            .build();

        client.put(policy, key, bins);

        Record updated = client.get(new Policy(), key);
        assertThat(updated.bins.get("notPresent")).isEqualTo("cats");
    }

    protected abstract boolean isEntityClassSetEmpty(Class<?> clazz);

    protected abstract void truncateSetOfEntityClass(Class<?> clazz);

    protected abstract boolean isSetEmpty(Class<?> clazz, String setName);

    protected abstract void truncateSet(String setName);

    protected abstract String getNamespace();

    protected abstract String getSetName(Class<?> clazz);

    @Value
    @Builder
    public static class ScanJob {

        @NonNull
        String module;
        String set;
        String udfFunction;
        @NonNull
        String status;
    }


    public abstract List<Customer> saveGeneratedCustomers(int count);

    public abstract List<Person> saveGeneratedPersons(int count);
}
