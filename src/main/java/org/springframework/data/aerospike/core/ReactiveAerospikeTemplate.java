/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.function.Supplier;

import static com.aerospike.client.ResultCode.KEY_NOT_FOUND_ERROR;
import static java.util.Objects.nonNull;
import static org.springframework.data.aerospike.core.OperationUtils.operations;

/**
 * Primary implementation of {@link ReactiveAerospikeOperations}.
 *
 * @author Igor Ermolenko
 * @author Volodymyr Shpynta
 * @author Yevhen Tsyba
 */
@Slf4j
public class ReactiveAerospikeTemplate extends BaseAerospikeTemplate implements ReactiveAerospikeOperations {

    private final AerospikeReactorClient reactorClient;
    private final ReactorQueryEngine queryEngine;
    private final ReactorIndexRefresher reactorIndexRefresher;

    public ReactiveAerospikeTemplate(AerospikeReactorClient reactorClient,
                                     String namespace,
                                     MappingAerospikeConverter converter,
                                     AerospikeMappingContext mappingContext,
                                     AerospikeExceptionTranslator exceptionTranslator,
                                     ReactorQueryEngine queryEngine, ReactorIndexRefresher reactorIndexRefresher) {
        super(namespace, converter, mappingContext, exceptionTranslator, reactorClient.getWritePolicyDefault());
        Assert.notNull(reactorClient, "Aerospike reactor client must not be null!");
        this.reactorClient = reactorClient;
        this.queryEngine = queryEngine;
        this.reactorIndexRefresher = reactorIndexRefresher;
    }

    @Override
    public <T> Mono<T> save(T document) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationCasAwareSavePolicy(data);

            return doPersistWithVersionAndHandleCasError(document, data, policy);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.REPLACE);

            return doPersistAndHandleError(document, data, policy);
        }
    }

    @Override
    public <T> Flux<T> insertAll(Collection<? extends T> documents) {
        return Flux.fromIterable(documents)
                .flatMap(this::insert);
    }

    @Override
    public <T> Mono<T> insert(T document) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);
        WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.CREATE_ONLY);

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            // we are ignoring generation here as insert operation should fail with DuplicateKeyException if key already exists
            // and we do not mind which initial version is set in the document, BUT we need to update the version value in the original document
            // also we do not want to handle aerospike error codes as cas aware error codes as we are ignoring generation
            return doPersistWithVersionAndHandleError(document, data, policy);
        } else {
            return doPersistAndHandleError(document, data, policy);
        }
    }

    @Override
    public <T> Mono<T> update(T document) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.REPLACE_ONLY);

            return doPersistWithVersionAndHandleCasError(document, data, policy);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.REPLACE_ONLY);

            return doPersistAndHandleError(document, data, policy);
        }
    }

    @Override
    public <T> Flux<T> findAll(Class<T> entityClass) {
        return findAllUsingQuery(entityClass, null, (Qualifier[]) null);
    }

    @Override
    public <T> Mono<T> add(T document, Map<String, Long> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(values, "Values must not be null!");

        AerospikeWriteData data = writeData(document);

        Operation[] operations = new Operation[values.size() + 1];
        int x = 0;
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            operations[x] = new Operation(Operation.Type.ADD, entry.getKey(), Value.get(entry.getValue()));
            x++;
        }
        operations[x] = Operation.get();

        WritePolicy writePolicy = WritePolicyBuilder.builder(this.writePolicyDefault)
                .expiration(data.getExpiration())
                .build();

        return executeOperationsOnValue(document, data, operations, writePolicy);
    }

    @Override
    public <T> Mono<T> add(T document, String binName, long value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");

        AerospikeWriteData data = writeData(document);

        WritePolicy writePolicy = WritePolicyBuilder.builder(this.writePolicyDefault)
                .expiration(data.getExpiration())
                .build();

        Operation[] operations = {Operation.add(new Bin(binName, value)), Operation.get(binName)};
        return executeOperationsOnValue(document, data, operations, writePolicy);
    }

    @Override
    public <T> Mono<T> append(T document, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(values, "Values must not be null!");

        AerospikeWriteData data = writeData(document);
        Operation[] operations = operations(values, Operation.Type.APPEND, Operation.get());
        return executeOperationsOnValue(document, data, operations, null);
    }

    @Override
    public <T> Mono<T> append(T document, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);
        Operation[] operations = {Operation.append(new Bin(binName, value)), Operation.get(binName)};
        return executeOperationsOnValue(document, data, operations, null);
    }

    @Override
    public <T> Mono<T> prepend(T document, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(values, "Values must not be null!");

        AerospikeWriteData data = writeData(document);
        Operation[] operations = operations(values, Operation.Type.PREPEND, Operation.get());
        return executeOperationsOnValue(document, data, operations, null);
    }

    @Override
    public <T> Mono<T> prepend(T document, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);
        Operation[] operations = {Operation.prepend(new Bin(binName, value)), Operation.get(binName)};
        return executeOperationsOnValue(document, data, operations, null);
    }

    private <T> Mono<T> executeOperationsOnValue(T document, AerospikeWriteData data, Operation[] operations, WritePolicy writePolicy) {
        return reactorClient.operate(writePolicy, data.getKey(), operations)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, getEntityClass(document), keyRecord.record))
                .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<T> findById(Object id, Class<T> entityClass) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        Key key = getKey(id, entity);

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for entity without expiration property");
            return getAndTouch(key, entity.getExpiration())
                    .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                    .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record))
                    .onErrorResume(
                            th -> th instanceof AerospikeException && ((AerospikeException) th).getResultCode() == KEY_NOT_FOUND_ERROR,
                            th -> Mono.empty()
                    )
                    .onErrorMap(this::translateError);
        } else {
            return reactorClient.get(key)
                    .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                    .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record))
                    .onErrorMap(this::translateError);
        }
    }

    @Override
    public <T> Flux<T> findByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

        return Flux.fromIterable(ids)
                .map(id -> getKey(id, entity))
                .flatMap(reactorClient::get)
                .filter(keyRecord -> nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record));
    }

    @Override
    public Mono<GroupedEntities> findByIds(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");

        if (groupedKeys.getEntitiesKeys().isEmpty()) {
            return Mono.just(GroupedEntities.builder().build());
        }

        return findEntitiesByIdsInternal(groupedKeys);
    }

    private Mono<GroupedEntities> findEntitiesByIdsInternal(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));

        return reactorClient.get(null, entitiesKeys.getKeys())
                .map(item -> toGroupedEntities(entitiesKeys, item.records))
                .onErrorMap(this::translateError);
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        return findAllUsingQuery(entityClass, query);
    }

    @Override
    public <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass) {
        Assert.notNull(entityClass, "Type for count must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        return findAllUsingQuery(entityClass, null, (Qualifier[]) null)
                .skip(offset)
                .take(limit);
    }

    @Override
    public <T> Mono<Long> count(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        return findAllRecordsUsingQuery(entityClass, query).count();
    }

    @Override
    public Mono<Long> count(String setName) {
        Assert.notNull(setName, "Set for count must not be null!");

        try {
             return Mono.fromCallable(() -> countSet(setName));
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> Mono<Long> count(Class<T> entityClass) {
        Assert.notNull(entityClass, "Type must not be null!");
        String setName = getSetName(entityClass);
        return count(setName);
    }

    private long countSet(String setName) {
        Node[] nodes = reactorClient.getAerospikeClient().getNodes();

        int replicationFactor = Utils.getReplicationFactor(nodes, this.namespace);

        long totalObjects = Arrays.stream(nodes)
                .mapToLong(node -> Utils.getObjectsCount(node, this.namespace, setName))
                .sum();

        return (nodes.length > 1) ? (totalObjects / replicationFactor) : totalObjects;
    }

    @Override
    public <T> Mono<T> execute(Supplier<T> supplier) {
        Assert.notNull(supplier, "Supplier must not be null!");

        return Mono.fromSupplier(supplier)
                .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Boolean> exists(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        Key key = getKey(id, entity);
        return reactorClient.exists(key)
                .map(Objects::nonNull)
                .defaultIfEmpty(false)
                .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Void> delete(Class<T> entityClass) {
        Assert.notNull(entityClass, "Type must not be null!");

        try {
            String set = getSetName(entityClass);
            return Mono.fromRunnable(() -> reactorClient.getAerospikeClient().truncate(null, this.namespace, set, null));
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> Mono<Boolean> delete(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

        return reactorClient
                .delete(ignoreGenerationDeletePolicy(), getKey(id, entity))
                .map(k -> true)
                .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Boolean> delete(T document) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);

        return this.reactorClient
                .delete(ignoreGenerationDeletePolicy(), data.getKey())
                .map(key -> true)
                .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                                String binName, IndexType indexType) {
        return createIndex(entityClass, indexName, binName, indexType, IndexCollectionType.DEFAULT);
    }

    @Override
    public <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                                      String binName, IndexType indexType, IndexCollectionType indexCollectionType) {
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");
        Assert.notNull(indexType, "Index type must not be null!");
        Assert.notNull(indexCollectionType, "Index collection type must not be null!");

        String setName = getSetName(entityClass);
        return reactorClient.createIndex(null, this.namespace,
                setName, indexName, binName, indexType, indexCollectionType)
                .then(reactorIndexRefresher.refreshIndexes())
                .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Void> deleteIndex(Class<T> entityClass, String indexName) {
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");

        String setName = getSetName(entityClass);
        return reactorClient.dropIndex(null, this.namespace, setName, indexName)
                .then(reactorIndexRefresher.refreshIndexes())
                .onErrorMap(this::translateError);
    }

    @Override
    public IAerospikeReactorClient getAerospikeReactorClient() {
        return reactorClient;
    }

    private <T> Mono<T> doPersistAndHandleError(T document, AerospikeWriteData data, WritePolicy policy) {
        return reactorClient
                .put(policy, data.getKey(), data.getBinsAsArray())
                .map(docKey -> document)
                .onErrorMap(this::translateError);
    }

    private <T> Mono<T> doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data, WritePolicy policy) {
        return putAndGetHeader(data, policy)
                .map(newRecord -> updateVersion(document, newRecord))
                .onErrorMap(AerospikeException.class, this::translateCasError);
    }

    private <T> Mono<T> doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy policy) {
        return putAndGetHeader(data, policy)
                .map(newRecord -> updateVersion(document, newRecord))
                .onErrorMap(AerospikeException.class, this::translateError);
    }

    private Mono<Record> putAndGetHeader(AerospikeWriteData data, WritePolicy policy) {
        Operation[] operations = operations(data.getBinsAsArray(), Operation::put, Operation.getHeader());

        return reactorClient.operate(policy, data.getKey(), operations)
                .map(keyRecord -> keyRecord.record);
    }

    private Mono<KeyRecord> getAndTouch(Key key, int expiration) {
        WritePolicy writePolicy = WritePolicyBuilder.builder(this.writePolicyDefault)
                .expiration(expiration)
                .build();
        return reactorClient.operate(writePolicy, key, Operation.touch(), Operation.get());
    }

    private Throwable translateError(Throwable e) {
        if (e instanceof AerospikeException) {
            return translateError((AerospikeException) e);
        }
        return e;
    }

    <T> Flux<T> findAllUsingQuery(Class<T> type, Query query) {
        if ((query.getSort() == null || query.getSort().isUnsorted())
                && query.getOffset() > 0) {
            throw new IllegalArgumentException("Unsorted query must not have offset value. " +
                    "For retrieving paged results use sorted query.");
        }

        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        Flux<T> results = findAllUsingQuery(type, null, qualifier);

        if (query.getSort() != null && query.getSort().isSorted()) {
            Comparator<T> comparator = getComparator(query);
            results = results.sort(comparator);
        }

        if (query.hasOffset()) {
            results = results.skip(query.getOffset());
        }
        if (query.hasRows()) {
            results = results.take(query.getRows());
        }
        return results;
    }

    <T> Flux<T> findAllUsingQuery(Class<T> type, Filter filter, Qualifier... qualifiers) {
        return findAllRecordsUsingQuery(type, filter, qualifiers)
                .map(keyRecord -> mapToEntity(keyRecord.key, type, keyRecord.record));
    }

    <T> Flux<KeyRecord> findAllRecordsUsingQuery(Class<T> type, Query query) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(type, "Type must not be null!");

        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        return findAllRecordsUsingQuery(type, null, qualifier);
    }

    <T> Flux<KeyRecord> findAllRecordsUsingQuery(Class<T> type, Filter filter, Qualifier... qualifiers) {
        String setName = getSetName(type);
        return this.queryEngine.select(this.namespace, setName, filter, qualifiers);
    }
}
