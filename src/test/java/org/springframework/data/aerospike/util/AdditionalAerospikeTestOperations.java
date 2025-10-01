package org.springframework.data.aerospike.util;

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
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.awaitility.Awaitility;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
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
@Slf4j
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
        String showCmd = "query-show";
        if (!serverVersionSupport.isSIndexCardinalitySupported()) {
            throw new UnsupportedOperationException("Minimal supported Aerospike Server version is 6.1");
        }
        Container.ExecResult execResult = aerospike.execInContainer("asinfo", "-v", showCmd);
        String stdout = execResult.getStdout();
        return getScanJobs(stdout);
    }

    private List<ScanJob> getScanJobs(String stdout) {
        if (stdout.isEmpty() || stdout.equals("\n")) {
            return Collections.emptyList();
        }
        String response = stdout.replaceAll("\n", "");
        return ResponseUtils.parseResponse(response, pairsMap -> ScanJob.builder()
            .set(pairsMap.get("set"))
            .udfFunction(pairsMap.get("udf-function"))
            .status(pairsMap.get("status"))
            .build());
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
        IndexUtils.createIndex(client, serverVersionSupport, namespace, setName, indexName, binName, indexType,
            collType,
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

    public void createIndexesAsyncBlocking(IAerospikeReactorClient reactorClient,
                                           ReactiveAerospikeTemplate reactiveTemplate,
                                           Collection<Index> indexesToBeCreated) {
        indexesToBeCreated.forEach(index -> {
                IndexCollectionType collType = index.getIndexCollectionType() == null
                    ? DEFAULT : index.getIndexCollectionType();
                CTX[] ctx = index.getCtx() == null
                    ? new CTX[0] : index.getCtx();
                log.info("Creating index " + index.getName() + ": ns " + getNamespace() + ", set " + index.getSet());
                reactiveTemplate.createIndex(index.getSet(), index.getName(), index.getBin(), index.getIndexType(),
                    collType, ctx).block();
                log.info("Created index " + index.getName() + ": ns " + getNamespace() + ", set " + index.getSet());
            }
        );
    }

    public <T> void dropIndex(String setName, String indexName) {
        IndexUtils.dropIndex(client, serverVersionSupport, getNamespace(), setName, indexName);
        indexesRefresher.refreshIndexesCache();
    }

    public <T> void dropIndex(Class<T> entityClass, String indexName) {
        dropIndex(getSetName(entityClass), indexName);
    }

    public void dropIndexes(Collection<Index> indexesToBeDropped) {
        indexesToBeDropped.forEach(index ->
            IndexUtils.dropIndex(client, serverVersionSupport, getNamespace(), index.getSet(), index.getName()));
        indexesRefresher.refreshIndexesCache();
    }

    public void dropIndexes(Class<?> entityClass) {
        String setName = getSetName(entityClass);
        List<Index> indexes = getIndexes(setName);
        if (!indexes.isEmpty()) {
            log.debug("Dropping {} indexes for set {}", indexes.size(), setName);
            indexes.forEach(index ->
                IndexUtils.dropIndex(client, serverVersionSupport, getNamespace(), index.getSet(), index.getName()));
            indexesRefresher.refreshIndexesCache();
        } else {
            log.debug("No indexes found for set {}", setName);
        }
    }

    public <T> void deleteAll(AerospikeRepository<T, ?> repository, Collection<T> entities) {
        try {
            repository.deleteAll(entities);
        } catch (AerospikeException.BatchRecordArray ignored) {
            // KEY_NOT_FOUND ResultCode causes exception if there are no entities
        }
    }

    public <T> void saveAll(AerospikeRepository<T, ?> repository, Collection<T> entities) {
        repository.saveAll(entities);
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

        String set;
        String udfFunction;
        @NonNull
        String status;
    }

    public abstract List<Customer> saveGeneratedCustomers(int count);

    public abstract List<Person> saveGeneratedPersons(int count);
}
