/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.KeyRecordIterator;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.OperationUtils.operations;

/**
 * Primary implementation of {@link AerospikeOperations}.
 *
 * @author Oliver Gierke
 * @author Peter Milne
 * @author Anastasiia Smirnova
 * @author Igor Ermolenko
 * @author Roman Terentiev
 */
@Slf4j
public class AerospikeTemplate extends BaseAerospikeTemplate implements AerospikeOperations {

    private final IAerospikeClient client;
    private final QueryEngine queryEngine;
    private final IndexRefresher indexRefresher;

    public AerospikeTemplate(IAerospikeClient client,
                             String namespace,
                             MappingAerospikeConverter converter,
                             AerospikeMappingContext mappingContext,
                             AerospikeExceptionTranslator exceptionTranslator,
                             QueryEngine queryEngine,
                             IndexRefresher indexRefresher) {
        super(namespace, converter, mappingContext, exceptionTranslator, client.getWritePolicyDefault());
        this.client = client;
        this.queryEngine = queryEngine;
        this.indexRefresher = indexRefresher;
    }

    @Override
    public <T> void createIndex(Class<T> entityClass, String indexName,
                                String binName, IndexType indexType) {
        createIndex(entityClass, indexName, binName, indexType, IndexCollectionType.DEFAULT);
    }

    @Override
    public <T> void createIndex(Class<T> entityClass, String indexName,
                                String binName, IndexType indexType, IndexCollectionType indexCollectionType) {
        createIndex(entityClass, indexName, binName, indexType, indexCollectionType, new CTX[0]);
    }

    @Override
    public <T> void createIndex(Class<T> entityClass, String indexName,
                                String binName, IndexType indexType, IndexCollectionType indexCollectionType,
                                CTX... ctx) {
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");
        Assert.notNull(indexType, "Index type must not be null!");
        Assert.notNull(indexCollectionType, "Index collection type must not be null!");
        Assert.notNull(ctx, "Ctx must not be null!");

        try {
            String setName = getSetName(entityClass);
            IndexTask task = client.createIndex(null, this.namespace,
                setName, indexName, binName, indexType, indexCollectionType, ctx);
            if (task != null) {
                task.waitTillComplete();
            }
            indexRefresher.refreshIndexes();
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> void deleteIndex(Class<T> entityClass, String indexName) {
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");

        try {
            String setName = getSetName(entityClass);
            IndexTask task = client.dropIndex(null, this.namespace, setName, indexName);
            if (task != null) {
                task.waitTillComplete();
            }
            indexRefresher.refreshIndexes();
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public boolean indexExists(String indexName) {
        Assert.notNull(indexName, "Index name must not be null!");
        log.warn("`indexExists` operation is deprecated. Please stop using it as it will be removed " +
            "in next major release.");

        try {
            Node[] nodes = client.getNodes();
            Node node = Utils.getRandomNode(nodes);
            String response = Info.request(node, "sindex/" + namespace + '/' + indexName);
            return !response.startsWith("FAIL:201");
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> void save(T document) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationCasAwareSavePolicy(data);

            doPersistWithVersionAndHandleCasError(document, data, policy);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.REPLACE);

            doPersistAndHandleError(data, policy);
        }
    }

    @Override
    public <T> void persist(T document, WritePolicy policy) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(policy, "Policy must not be null!");

        AerospikeWriteData data = writeData(document);

        doPersistAndHandleError(data, policy);
    }

    @Override
    public <T> void insertAll(Collection<? extends T> documents) {
        Assert.notNull(documents, "Documents must not be null!");

        documents.stream().filter(Objects::nonNull).forEach(this::insert);
    }

    @Override
    public <T> void insert(T document) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);
        WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.CREATE_ONLY);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            // we are ignoring generation here as insert operation should fail with DuplicateKeyException if key
            // already exists
            // we do not mind which initial version is set in the document, BUT we need to update the version value
            // in the original document
            // also we do not want to handle aerospike error codes as cas aware error codes as we are ignoring
            // generation
            doPersistWithVersionAndHandleError(document, data, policy);
        } else {
            doPersistAndHandleError(data, policy);
        }
    }

    @Override
    public <T> void update(T document) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.REPLACE_ONLY);

            doPersistWithVersionAndHandleCasError(document, data, policy);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.REPLACE_ONLY);

            doPersistAndHandleError(data, policy);
        }
    }

    @Override
    public <T> void update(T document, Collection<String> fields) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeDataWithSpecificFields(document, fields);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            doPersistWithVersionAndHandleCasError(document, data, policy);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            doPersistAndHandleError(data, policy);
        }
    }

    @Override
    public <T> void delete(Class<T> entityClass) {
        Assert.notNull(entityClass, "Type must not be null!");

        try {
            String set = getSetName(entityClass);
            client.truncate(null, getNamespace(), set, null);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> boolean delete(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        try {
            AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
            Key key = getKey(id, entity);

            return this.client.delete(ignoreGenerationDeletePolicy(), key);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> boolean delete(T document) {
        Assert.notNull(document, "Document must not be null!");

        try {
            AerospikeWriteData data = writeData(document);

            return this.client.delete(ignoreGenerationDeletePolicy(), data.getKey());
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> boolean exists(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        try {
            AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
            Key key = getKey(id, entity);

            Record aeroRecord = this.client.operate(null, key, Operation.getHeader());
            return aeroRecord != null;
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Stream<T> findAll(Class<T> entityClass) {
        Assert.notNull(entityClass, "Type must not be null!");

        return (Stream<T>) findAllUsingQuery(entityClass, null, null, (Qualifier[]) null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> Stream<S> findAll(Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(targetClass, "Target type must not be null!");

        return (Stream<S>) findAllUsingQuery(entityClass, targetClass, null, (Qualifier[]) null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T findById(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        return (T) findByIdInternal(id, entityClass, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(targetClass, "Target type must not be null!");

        return (S) findByIdInternal(id, entityClass, targetClass);
    }

    private <T, S> Object findByIdInternal(Object id, Class<T> entityClass, Class<S> targetClass) {
        try {
            AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
            Key key = getKey(id, entity);

            if (targetClass != null) {
                return getRecordMapToTargetClass(entity, key, targetClass);
            } else {
                return getRecordMapToEntityClass(entity, key, entityClass);
            }
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private <S> Object getRecordMapToTargetClass(AerospikePersistentEntity<?> entity, Key key, Class<S> targetClass) {
        Record aeroRecord;
        String[] binNames = getBinNamesFromTargetClass(targetClass);
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), binNames);
        } else {
            aeroRecord = this.client.get(null, key, binNames);
        }
        return mapToEntity(key, targetClass, aeroRecord);
    }

    private <T> Object getRecordMapToEntityClass(AerospikePersistentEntity<?> entity, Key key, Class<T> entityClass) {
        Record aeroRecord;
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), null);
        } else {
            aeroRecord = this.client.get(null, key);
        }
        return mapToEntity(key, entityClass, aeroRecord);
    }

    private Record getAndTouch(Key key, int expiration, String[] binNames) {
        WritePolicy writePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
            .expiration(expiration)
            .build();

        try {
            if (binNames == null || binNames.length == 0) {
                return this.client.operate(writePolicy, key, Operation.touch(), Operation.get());
            } else {
                Operation[] operations = new Operation[binNames.length + 1];
                operations[0] = Operation.touch();

                for (int i = 1; i < operations.length; i++) {
                    operations[i] = Operation.get(binNames[i - 1]);
                }
                return this.client.operate(writePolicy, key, operations);
            }
        } catch (AerospikeException aerospikeException) {
            if (aerospikeException.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                return null;
            }
            throw aerospikeException;
        }
    }

    private String[] getBinNamesFromTargetClass(Class<?> targetClass) {
        AerospikePersistentEntity<?> targetEntity = mappingContext.getRequiredPersistentEntity(targetClass);

        List<String> binNamesList = new ArrayList<>();

        targetEntity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) property
            -> binNamesList.add(property.getFieldName()));

        return binNamesList.toArray(new String[0]);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        return (List<T>) findByIdsInternal(IterableConverter.toList(ids), entityClass, null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(targetClass, "Target type must not be null!");

        return (List<S>) findByIdsInternal(IterableConverter.toList(ids), entityClass, targetClass);
    }

    private <T, S> List<?> findByIdsInternal(Collection<?> ids, Class<T> entityClass, Class<S> targetClass) {
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

            Key[] keys = ids.stream()
                .map(id -> getKey(id, entity))
                .toArray(Key[]::new);

            if (targetClass != null) {
                String[] binNames = getBinNamesFromTargetClass(targetClass);
                Record[] aeroRecords = client.get(null, keys, binNames);

                return IntStream.range(0, keys.length)
                    .filter(index -> aeroRecords[index] != null)
                    .mapToObj(index -> mapToEntity(keys[index], targetClass, aeroRecords[index]))
                    .collect(Collectors.toList());
            } else {
                Record[] aeroRecords = client.get(null, keys);

                return IntStream.range(0, keys.length)
                    .filter(index -> aeroRecords[index] != null)
                    .mapToObj(index -> mapToEntity(keys[index], entityClass, aeroRecords[index]))
                    .collect(Collectors.toList());
            }
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public GroupedEntities findByIds(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");

        if (groupedKeys.getEntitiesKeys().isEmpty()) {
            return GroupedEntities.builder().build();
        }

        return findEntitiesByIdsInternal(groupedKeys);
    }

    private GroupedEntities findEntitiesByIdsInternal(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));
        Record[] aeroRecords = client.get(null, entitiesKeys.getKeys());

        return toGroupedEntities(entitiesKeys, aeroRecords);
    }

    @Override
    public <T> ResultSet aggregate(Filter filter, Class<T> entityClass,
                                   String module, String function, List<Value> arguments) {
        Assert.notNull(entityClass, "Type must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

        Statement statement = new Statement();
        if (filter != null)
            statement.setFilter(filter);
        statement.setSetName(entity.getSetName());
        statement.setNamespace(this.namespace);
        ResultSet resultSet;
        if (arguments != null && arguments.size() > 0)
            resultSet = this.client.queryAggregate(null, statement, module,
                function, arguments.toArray(new Value[0]));
        else
            resultSet = this.client.queryAggregate(null, statement);
        return resultSet;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Stream<T> findAll(Sort sort, long offset, long limit, Class<T> entityClass) {
        Assert.notNull(entityClass, "Type must not be null!");

        return (Stream<T>) findAllUsingQueryWithPostProcessing(entityClass, null, sort, offset, limit,
            null, (Qualifier[]) null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> Stream<S> findAll(Sort sort, long offset, long limit, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(targetClass, "Target type must not be null!");

        return (Stream<S>) findAllUsingQueryWithPostProcessing(entityClass, targetClass, sort, offset, limit,
            null, (Qualifier[]) null);
    }

    public <T> boolean exists(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Type must not be null!");

        return find(query, entityClass).findAny().isPresent();
    }

    @Override
    public <T> T execute(Supplier<T> supplier) {
        Assert.notNull(supplier, "Supplier must not be null!");

        try {
            return supplier.get();
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> long count(Query query, Class<T> entityClass) {
        Assert.notNull(entityClass, "Type must not be null!");

        Stream<KeyRecord> results = findAllRecordsUsingQuery(entityClass, query);
        return results.count();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Stream<T> find(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        return (Stream<T>) findAllUsingQueryWithPostProcessing(entityClass, null, query);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> Stream<S> find(Query query, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");
        Assert.notNull(targetClass, "Target type must not be null!");

        return (Stream<S>) findAllUsingQueryWithPostProcessing(entityClass, targetClass, query);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Stream<T> findInRange(long offset, long limit, Sort sort,
                                     Class<T> entityClass) {
        Assert.notNull(entityClass, "Type for count must not be null!");

        return (Stream<T>) findAllUsingQueryWithPostProcessing(entityClass, null, sort, offset, limit,
            null, (Qualifier[]) null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> Stream<S> findInRange(long offset, long limit, Sort sort,
                                        Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Type for count must not be null!");
        Assert.notNull(targetClass, "Target type must not be null!");

        return (Stream<S>) findAllUsingQueryWithPostProcessing(entityClass, targetClass, sort, offset, limit,
            null, (Qualifier[]) null);
    }

    @Override
    public <T> long count(Class<T> entityClass) {
        Assert.notNull(entityClass, "Type for count must not be null!");

        String setName = getSetName(entityClass);
        return count(setName);
    }

    @Override
    public IAerospikeClient getAerospikeClient() {
        return client;
    }

    @Override
    public long count(String setName) {
        Assert.notNull(setName, "Set for count must not be null!");

        try {
            Node[] nodes = client.getNodes();

            int replicationFactor = Utils.getReplicationFactor(nodes, namespace);

            long totalObjects = Arrays.stream(nodes)
                .mapToLong(node -> Utils.getObjectsCount(node, namespace, setName))
                .sum();

            return (nodes.length > 1) ? (totalObjects / replicationFactor) : totalObjects;
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T prepend(T document, String fieldName, String value) {
        Assert.notNull(document, "Document must not be null!");

        try {
            AerospikeWriteData data = writeData(document);
            Record aeroRecord = this.client.operate(null, data.getKey(),
                Operation.prepend(new Bin(fieldName, value)),
                Operation.get(fieldName));

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T prepend(T document, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(values, "Values must not be null!");

        try {
            AerospikeWriteData data = writeData(document);
            Operation[] ops = operations(values, Operation.Type.PREPEND, Operation.get());
            Record aeroRecord = this.client.operate(null, data.getKey(), ops);

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T append(T document, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(values, "Values must not be null!");

        try {
            AerospikeWriteData data = writeData(document);
            Operation[] ops = operations(values, Operation.Type.APPEND, Operation.get());
            Record aeroRecord = this.client.operate(null, data.getKey(), ops);

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T append(T document, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");

        try {
            AerospikeWriteData data = writeData(document);
            Record aeroRecord = this.client.operate(null, data.getKey(),
                Operation.append(new Bin(binName, value)),
                Operation.get(binName));

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T add(T document, Map<String, Long> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(values, "Values must not be null!");

        try {
            AerospikeWriteData data = writeData(document);
            Operation[] ops = operations(values, Operation.Type.ADD, Operation.get());

            WritePolicy writePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
                .expiration(data.getExpiration())
                .build();

            Record aeroRecord = this.client.operate(writePolicy, data.getKey(), ops);

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T add(T document, String binName, long value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");

        try {
            AerospikeWriteData data = writeData(document);

            WritePolicy writePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
                .expiration(data.getExpiration())
                .build();

            Record aeroRecord = this.client.operate(writePolicy, data.getKey(),
                Operation.add(new Bin(binName, value)), Operation.get());

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private void doPersistAndHandleError(AerospikeWriteData data, WritePolicy policy) {
        try {
            put(data, policy);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private <T> void doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data, WritePolicy policy) {
        try {
            Record newAeroRecord = putAndGetHeader(data, policy);
            updateVersion(document, newAeroRecord);
        } catch (AerospikeException e) {
            throw translateCasError(e);
        }
    }

    private <T> void doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy policy) {
        try {
            Record newAeroRecord = putAndGetHeader(data, policy);
            updateVersion(document, newAeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private void put(AerospikeWriteData data, WritePolicy policy) {
        Key key = data.getKey();
        Bin[] bins = data.getBinsAsArray();

        client.put(policy, key, bins);
    }

    private Record putAndGetHeader(AerospikeWriteData data, WritePolicy policy) {
        Key key = data.getKey();
        Bin[] bins = data.getBinsAsArray();

        if (bins.length == 0) {
            throw new AerospikeException(
                "Cannot put and get header on a document with no bins and \"@_class\" bin disabled.");
        }

        Operation[] operations = operations(bins, Operation::put, Operation.getHeader());

        return client.operate(policy, key, operations);
    }

    <T, S> Stream<?> findAllUsingQueryWithPostProcessing(Class<T> entityClass, Class<S> targetClass, Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        Stream<?> results = findAllUsingQuery(entityClass, targetClass, null, qualifier);
        return applyPostProcessingOnResults(results, query);
    }

    @SuppressWarnings("SameParameterValue")
    <T, S> Stream<?> findAllUsingQueryWithPostProcessing(Class<T> entityClass, Class<S> targetClass, Sort sort,
                                                         long offset, long limit, Filter filter,
                                                         Qualifier... qualifiers) {
        verifyUnsortedWithOffset(sort, offset);
        Stream<?> results = findAllUsingQuery(entityClass, targetClass, filter, qualifiers);
        return applyPostProcessingOnResults(results, sort, offset, limit);
    }

    private void verifyUnsortedWithOffset(Sort sort, long offset) {
        if ((sort == null || sort.isUnsorted())
            && offset > 0) {
            throw new IllegalArgumentException("Unsorted query must not have offset value. " +
                "For retrieving paged results use sorted query.");
        }
    }

    <T, S> Stream<?> findAllUsingQuery(Class<T> entityClass, Class<S> targetClass, Filter filter,
                                       Qualifier... qualifiers) {
        return findAllRecordsUsingQuery(entityClass, targetClass, filter, qualifiers)
            .map(keyRecord -> {
                if (targetClass != null) {
                    return mapToEntity(keyRecord.key, targetClass, keyRecord.record);
                }
                return mapToEntity(keyRecord.key, entityClass, keyRecord.record);
            });
    }

    private <T> Stream<T> applyPostProcessingOnResults(Stream<T> results, Query query) {
        if (query.getSort() != null && query.getSort().isSorted()) {
            Comparator<T> comparator = getComparator(query);
            results = results.sorted(comparator);
        }
        if (query.hasOffset()) {
            results = results.skip(query.getOffset());
        }
        if (query.hasRows()) {
            results = results.limit(query.getRows());
        }

        return results;
    }

    private <T> Stream<T> applyPostProcessingOnResults(Stream<T> results, Sort sort, long offset, long limit) {
        if (sort != null && sort.isSorted()) {
            Comparator<T> comparator = getComparator(sort);
            results = results.sorted(comparator);
        }

        if (offset > 0) {
            results = results.skip(offset);
        }

        if (limit > 0) {
            results = results.limit(limit);
        }
        return results;
    }

    <T> Stream<KeyRecord> findAllRecordsUsingQuery(Class<T> entityClass, Query query) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Type must not be null!");

        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        return findAllRecordsUsingQuery(entityClass, null, null, qualifier);
    }

    <T, S> Stream<KeyRecord> findAllRecordsUsingQuery(Class<T> entityClass, Class<S> targetClass, Filter filter,
                                                      Qualifier... qualifiers) {
        String setName = getSetName(entityClass);

        KeyRecordIterator recIterator;

        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass);
            recIterator = queryEngine.select(namespace, setName, binNames, filter, qualifiers);
        } else {
            recIterator = queryEngine.select(namespace, setName, filter, qualifiers);
        }

        return StreamUtils.createStreamFromIterator(recIterator)
            .onClose(() -> {
                try {
                    recIterator.close();
                } catch (Exception e) {
                    log.error("Caught exception while closing query", e);
                }
            });
    }
}
