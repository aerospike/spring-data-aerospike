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

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
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
import org.springframework.data.aerospike.index.IndexesCacheRefresher;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.CoreUtils.getDistinctPredicate;
import static org.springframework.data.aerospike.core.CoreUtils.operations;
import static org.springframework.data.aerospike.core.CoreUtils.verifyUnsortedWithOffset;
import static org.springframework.data.aerospike.core.TemplateUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.core.TemplateUtils.getIdValue;
import static org.springframework.data.aerospike.query.QualifierUtils.getOneIdQualifier;
import static org.springframework.data.aerospike.query.QualifierUtils.validateQualifiers;
import static org.springframework.data.aerospike.utility.Utils.allArrayElementsAreNull;

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
public class AerospikeTemplate extends BaseAerospikeTemplate implements AerospikeOperations,
    AerospikeInternalOperations, IndexesCacheRefresher {

    private static final Pattern INDEX_EXISTS_REGEX_PATTERN = Pattern.compile("^FAIL:(-?\\d+).*$");
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
    public IAerospikeClient getAerospikeClient() {
        return client;
    }

    @Override
    public <T> void createIndex(Class<T> entityClass, String indexName,
                                String binName, IndexType indexType) {
        Assert.notNull(entityClass, "Class must not be null!");
        createIndex(entityClass, indexName, binName, indexType, IndexCollectionType.DEFAULT);
    }

    @Override
    public <T> void createIndex(Class<T> entityClass, String indexName,
                                String binName, IndexType indexType, IndexCollectionType indexCollectionType) {
        Assert.notNull(entityClass, "Class must not be null!");
        createIndex(entityClass, indexName, binName, indexType, indexCollectionType, new CTX[0]);
    }

    @Override
    public <T> void createIndex(Class<T> entityClass, String indexName,
                                String binName, IndexType indexType, IndexCollectionType indexCollectionType,
                                CTX... ctx) {
        Assert.notNull(entityClass, "Class must not be null!");
        createIndex(getSetName(entityClass), indexName, binName, indexType, indexCollectionType, ctx);
    }

    @Override
    public void createIndex(String setName, String indexName,
                            String binName, IndexType indexType) {
        createIndex(setName, indexName, binName, indexType, IndexCollectionType.DEFAULT);
    }

    @Override
    public void createIndex(String setName, String indexName, String binName, IndexType indexType,
                            IndexCollectionType indexCollectionType) {
        createIndex(setName, indexName, binName, indexType, indexCollectionType, new CTX[0]);
    }

    @Override
    public void createIndex(String setName, String indexName, String binName,
                            IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx) {
        Assert.notNull(setName, "Set name type must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");
        Assert.notNull(indexType, "Index type must not be null!");
        Assert.notNull(indexCollectionType, "Index collection type must not be null!");
        Assert.notNull(ctx, "Ctx must not be null!");

        try {
            IndexTask task = client.createIndex(null, this.namespace,
                setName, indexName, binName, indexType, indexCollectionType, ctx);
            if (task != null) {
                task.waitTillComplete();
            }
            refreshIndexesCache();
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public void refreshIndexesCache() {
        indexRefresher.refreshIndexes();
    }

    @Override
    public <T> void deleteIndex(Class<T> entityClass, String indexName) {
        Assert.notNull(entityClass, "Class must not be null!");
        deleteIndex(getSetName(entityClass), indexName);
    }

    @Override
    public void deleteIndex(String setName, String indexName) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");

        try {
            IndexTask task = client.dropIndex(null, this.namespace, setName, indexName);
            if (task != null) {
                task.waitTillComplete();
            }
            refreshIndexesCache();
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public boolean indexExists(String indexName) {
        Assert.notNull(indexName, "Index name must not be null!");

        try {
            Node[] nodes = client.getNodes();
            for (Node node : nodes) {
                String response = Info.request(node, "sindex-exists:ns=" + namespace + ";indexname=" + indexName);
                if (response == null) throw new AerospikeException("Null node response");

                if (response.equalsIgnoreCase("true")) {
                    return true;
                } else if (response.equalsIgnoreCase("false")) {
                    return false;
                } else {
                    Matcher matcher = INDEX_EXISTS_REGEX_PATTERN.matcher(response);
                    if (matcher.matches()) {
                        int reason;
                        try {
                            reason = Integer.parseInt(matcher.group(1));
                        } catch (NumberFormatException e) {
                            throw new AerospikeException("Unexpected node response, unable to parse ResultCode: " +
                                response);
                        }

                        // as for Server ver. >= 6.1.0.1 the response containing ResultCode.INVALID_NAMESPACE
                        // means that the request should be sent to another node
                        if (reason != ResultCode.INVALID_NAMESPACE) {
                            throw new AerospikeException(reason);
                        }
                    } else {
                        throw new AerospikeException("Unexpected node response: " + response);
                    }
                }
            }
        } catch (AerospikeException e) {
            throw translateError(e);
        }
        return false;
    }

    @Override
    public <T> void save(T document) {
        Assert.notNull(document, "Document must not be null!");
        save(document, getSetName(document));
    }

    @Override
    public <T> void save(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationCasAwareSavePolicy(data);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            doPersistWithVersionAndHandleCasError(document, data, policy, true);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.UPDATE);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
            doPersistAndHandleError(data, policy, operations);
        }
    }

    @Override
    public <T> void saveAll(Iterable<T> documents) {
        Assert.notNull(documents, "Documents for saving must not be null!");
        saveAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void saveAll(Iterable<T> documents, String setName) {
        Assert.notNull(documents, "Documents for saving must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        documents.forEach(document -> batchWriteDataList.add(getBatchWriteForSave(document, setName)));

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();
        try {
            client.operate(null, batchWriteRecords);
        } catch (AerospikeException e) {
            throw translateError(e);
        }

        checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, "save");
    }

    private <T> void checkForErrorsAndUpdateVersion(List<BatchWriteData<T>> batchWriteDataList,
                                                    List<BatchRecord> batchWriteRecords, String commandName) {
        boolean errorsFound = false;
        for (AerospikeTemplate.BatchWriteData<T> data : batchWriteDataList) {
            if (!errorsFound && batchRecordFailed(data.batchRecord())) {
                errorsFound = true;
            }
            if (data.hasVersionProperty() && !batchRecordFailed(data.batchRecord())) {
                updateVersion(data.document(), data.batchRecord().record);
            }
        }

        if (errorsFound) {
            AerospikeException e = new AerospikeException("Errors during batch " + commandName);
            throw new AerospikeException.BatchRecordArray(batchWriteRecords.toArray(BatchRecord[]::new), e);
        }
    }

    @Override
    public <T> void persist(T document, WritePolicy policy) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(policy, "Policy must not be null!");
        persist(document, policy, getSetName(document));
    }

    @Override
    public <T> void persist(T document, WritePolicy policy, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(policy, "Policy must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
        doPersistAndHandleError(data, policy, operations);
    }

    @Override
    public <T> void insert(T document) {
        Assert.notNull(document, "Document must not be null!");
        insert(document, getSetName(document));
    }

    @Override
    public <T> void insert(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
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
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            doPersistAndHandleError(data, policy, operations);
        }
    }

    @Override
    public <T> void insertAll(Iterable<? extends T> documents) {
        Assert.notNull(documents, "Documents must not be null!");
        insertAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void insertAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(documents, "Documents for inserting must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        documents.forEach(document -> batchWriteDataList.add(getBatchWriteForInsert(document, setName)));

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();
        try {
            client.operate(null, batchWriteRecords);
        } catch (AerospikeException e) {
            throw translateError(e);
        }

        checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, "insert");
    }

    @Override
    public <T> void update(T document) {
        Assert.notNull(document, "Document must not be null!");
        update(document, getSetName(document));
    }

    @Override
    public <T> void update(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            doPersistWithVersionAndHandleCasError(document, data, policy, true);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = Stream.concat(Stream.of(Operation.delete()), data.getBins().stream()
                .map(Operation::put)).toArray(Operation[]::new);
            doPersistAndHandleError(data, policy, operations);
        }
    }

    @Override
    public <T> void update(T document, Collection<String> fields) {
        Assert.notNull(document, "Document must not be null!");
        update(document, getSetName(document), fields);
    }

    @Override
    public <T> void update(T document, String setName, Collection<String> fields) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeDataWithSpecificFields(document, setName, fields);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            doPersistWithVersionAndHandleCasError(document, data, policy, false);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            doPersistAndHandleError(data, policy, operations);
        }
    }

    @Override
    public <T> void updateAll(Iterable<T> documents) {
        Assert.notNull(documents, "Documents must not be null!");
        updateAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void updateAll(Iterable<T> documents, String setName) {
        Assert.notNull(documents, "Documents must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        documents.forEach(document -> batchWriteDataList.add(getBatchWriteForUpdate(document, setName)));

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();
        try {
            client.operate(null, batchWriteRecords);
        } catch (AerospikeException e) {
            throw translateError(e);
        }

        checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, "update");
    }

    @Override
    public <T> void delete(Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        delete(getSetName(entityClass));
    }

    @Override
    public void delete(String setName) {
        Assert.notNull(setName, "Set name must not be null!");

        try {
            client.truncate(null, getNamespace(), setName, null);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> boolean delete(Object id, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return delete(getSetName(entityClass), id);
    }

    // TODO: solve ambiguous param
    // id and setName param order must be reversed since it collides with delete(T document, String setName)
    //@Override
    private boolean delete(String setName, Object id) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        try {
            Key key = getKey(id, setName);

            return this.client.delete(ignoreGenerationDeletePolicy(), key);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> boolean delete(T document) {
        Assert.notNull(document, "Document must not be null!");
        return delete(document, getSetName(document));
    }

    @Override
    public <T> boolean delete(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        try {
            AerospikeWriteData data = writeData(document, setName);

            return this.client.delete(ignoreGenerationDeletePolicy(), data.getKey());
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> void deleteByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        deleteByIds(ids, getSetName(entityClass));
    }

    @Override
    public void deleteByIds(Iterable<?> ids, String setName) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        deleteByIdsInternal(IterableConverter.toList(ids), setName);
    }

    @Override
    public <T> void deleteByIdsInternal(Collection<?> ids, Class<T> entityClass) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        deleteByIdsInternal(ids, getSetName(entityClass));
    }

    @Override
    public void deleteByIdsInternal(Collection<?> ids, String setName) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return;
        }

        Key[] keys = ids.stream()
            .map(id -> getKey(id, setName))
            .toArray(Key[]::new);

        checkForErrors(client, keys);
    }

    private void checkForErrors(IAerospikeClient client, Key[] keys) {
        BatchResults results;
        try {
            // requires server ver. >= 6.0.0
            results = client.delete(null, null, keys);
        } catch (AerospikeException e) {
            throw translateError(e);
        }

        for (int i = 0; i < results.records.length; i++) {
            BatchRecord record = results.records[i];
            if (batchRecordFailed(record)) {
                throw new AerospikeException.BatchRecordArray(results.records,
                    new AerospikeException("Errors during batch delete"));
            }
        }
    }

    @Override
    public void deleteByIds(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");
        Assert.notNull(groupedKeys.getEntitiesKeys(), "Entities keys must not be null!");
        Assert.notEmpty(groupedKeys.getEntitiesKeys(), "Entities keys must not be empty!");

        deleteEntitiesByIdsInternal(groupedKeys);
    }

    private void deleteEntitiesByIdsInternal(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));
        checkForErrors(client, entitiesKeys.getKeys());
    }

    @Override
    public <T> boolean exists(Object id, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return exists(id, getSetName(entityClass));
    }

    @Override
    public boolean exists(Object id, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        try {
            Key key = getKey(id, setName);

            Record aeroRecord = this.client.operate(null, key, Operation.getHeader());
            return aeroRecord != null;
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> Stream<T> findAll(Class<T> entityClass) {
        Assert.notNull(entityClass, "Entity class must not be null!");

        return findAll(entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Stream<S> findAll(Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(targetClass, "Target class must not be null!");

        return findAll(targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Stream<T> findAll(Class<T> targetClass, String setName) {
        Assert.notNull(targetClass, "Target class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return findAllUsingQuery(targetClass, setName, null, null);
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return findById(id, entityClass, getSetName(entityClass));
    }

    @Override
    public <T> T findById(Object id, Class<T> entityClass, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return findById(id, entityClass, null, setName);
    }

    @Override
    public <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return findById(id, entityClass, targetClass, getSetName(entityClass));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return (S) findByIdInternal(id, entityClass, targetClass, setName);
    }

    @Override
    public <T, S> Object findByIdInternal(Object id, Class<T> entityClass, Class<S> targetClass,
                                          Qualifier... qualifiers) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return findByIdInternal(id, entityClass, targetClass, getSetName(entityClass), qualifiers);
    }

    @Override
    public <T, S> Object findByIdInternal(Object id, Class<T> entityClass, Class<S> targetClass, String setName,
                                          Qualifier... qualifiers) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        try {
            AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
            Key key = getKey(id, setName);

            if (targetClass != null && targetClass != entityClass) {
                return getRecordMapToTargetClass(entity, key, targetClass, qualifiers);
            }
            return mapToEntity(key, entityClass, getRecord(entity, key, qualifiers));
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T, S> List<?> findByIdsInternal(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                            Qualifier... qualifiers) {
        Assert.notNull(entityClass, "Class must not be null!");
        return findByIdsInternal(ids, entityClass, targetClass, getSetName(entityClass), qualifiers);
    }

    @Override
    public <T, S> List<?> findByIdsInternal(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                            String setName, Qualifier... qualifiers) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            Key[] keys = ids.stream()
                .map(id -> getKey(id, setName))
                .toArray(Key[]::new);

            BatchPolicy policy = getBatchPolicyFilterExp(qualifiers);

            Class<?> target;
            Record[] aeroRecords;
            if (targetClass != null && targetClass != entityClass) {
                String[] binNames = getBinNamesFromTargetClass(targetClass);
                aeroRecords = getAerospikeClient().get(policy, keys, binNames);
                target = targetClass;
            } else {
                aeroRecords = getAerospikeClient().get(policy, keys);
                target = entityClass;
            }

            return IntStream.range(0, keys.length)
                .filter(index -> aeroRecords[index] != null)
                .mapToObj(index -> mapToEntity(keys[index], target, aeroRecords[index]))
                .collect(Collectors.toList());
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private List<KeyRecord> findByIdsInternalWithoutMapping(Collection<?> ids, String setName,
                                                            Class<?> targetClass,
                                                            Qualifier... qualifiers) {
        Assert.notNull(ids, "Ids must not be null");
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            Key[] keys = getKeys(ids, setName);

            BatchPolicy policy = getBatchPolicyFilterExp(qualifiers);

            Record[] aeroRecords;
            if (targetClass != null) {
                String[] binNames = getBinNamesFromTargetClass(targetClass);
                aeroRecords = getAerospikeClient().get(policy, keys, binNames);
            } else {
                aeroRecords = getAerospikeClient().get(policy, keys);
            }

            return IntStream.range(0, keys.length)
                .filter(index -> aeroRecords[index] != null)
                .mapToObj(index -> new KeyRecord(keys[index], aeroRecords[index]))
                .collect(Collectors.toList());
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private BatchPolicy getBatchPolicyFilterExp(Qualifier[] qualifiers) {
        if (qualifiers != null && qualifiers.length > 0) {
            BatchPolicy policy = new BatchPolicy(getAerospikeClient().getBatchPolicyDefault());
            policy.filterExp = queryEngine.getFilterExpressionsBuilder().build(qualifiers);
            return policy;
        }
        return null;
    }

    private Key[] getKeys(Collection<?> ids, String setName) {
        return ids.stream()
            .map(id -> getKey(id, setName))
            .toArray(Key[]::new);
    }

    <S> Object getRecordMapToTargetClass(AerospikePersistentEntity<?> entity, Key key, Class<S> targetClass,
                                         Qualifier... qualifiers) {
        Record aeroRecord;
        String[] binNames = getBinNamesFromTargetClass(targetClass);
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), binNames, qualifiers);
        } else {
            Policy policy = getPolicyFilterExp(qualifiers);
            aeroRecord = getAerospikeClient().get(policy, key, binNames);
        }
        return mapToEntity(key, targetClass, aeroRecord);
    }

    private Policy getPolicyFilterExp(Qualifier[] qualifiers) {
        if (qualifiers != null && qualifiers.length > 0) {
            Policy policy = new Policy(getAerospikeClient().getReadPolicyDefault());
            policy.filterExp = queryEngine.getFilterExpressionsBuilder().build(qualifiers);
            return policy;
        }
        return null;
    }

    Record getRecord(AerospikePersistentEntity<?> entity, Key key, Qualifier... qualifiers) {
        Record aeroRecord;
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), null);
        } else {
            Policy policy = getPolicyFilterExp(qualifiers);
            aeroRecord = getAerospikeClient().get(policy, key);
        }
        return aeroRecord;
    }

    Record getAndTouch(Key key, int expiration, String[] binNames, Qualifier... qualifiers) {
        WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(client.getWritePolicyDefault())
            .expiration(expiration);

        if (qualifiers != null && qualifiers.length > 0) {
            writePolicyBuilder.filterExp(queryEngine.getFilterExpressionsBuilder().build(qualifiers));
        }
        WritePolicy writePolicy = writePolicyBuilder.build();

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

    String[] getBinNamesFromTargetClass(Class<?> targetClass) {
        AerospikePersistentEntity<?> targetEntity = mappingContext.getRequiredPersistentEntity(targetClass);

        List<String> binNamesList = new ArrayList<>();

        targetEntity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) property
            -> binNamesList.add(property.getFieldName()));

        return binNamesList.toArray(new String[0]);
    }

    @Override
    public <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass) {
        return findByIds(ids, entityClass, getSetName(entityClass));
    }

    @Override
    public <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass, String setName) {
        return findByIds(ids, entityClass, null, setName);
    }

    @Override
    public <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass) {
        return findByIds(ids, entityClass, targetClass, getSetName(entityClass));
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass, String setName) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return (List<S>) findByIdsInternal(IterableConverter.toList(ids), entityClass, targetClass, setName);
    }

    @Override
    public GroupedEntities findByIds(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");

        if (groupedKeys.getEntitiesKeys() == null || groupedKeys.getEntitiesKeys().isEmpty()) {
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
        return aggregate(filter, getSetName(entityClass), module, function, arguments);
    }

    @Override
    public ResultSet aggregate(Filter filter, String setName,
                               String module, String function, List<Value> arguments) {
        Assert.notNull(setName, "Set name must not be null!");

        Statement statement = new Statement();
        if (filter != null)
            statement.setFilter(filter);
        statement.setSetName(setName);
        statement.setNamespace(this.namespace);
        ResultSet resultSet;
        if (arguments != null && !arguments.isEmpty())
            resultSet = this.client.queryAggregate(null, statement, module,
                function, arguments.toArray(new Value[0]));
        else
            resultSet = this.client.queryAggregate(null, statement);
        return resultSet;
    }

    @Override
    public <T> Stream<T> findAll(Sort sort, long offset, long limit, Class<T> entityClass) {
        return findAll(sort, offset, limit, entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Stream<S> findAll(Sort sort, long offset, long limit, Class<T> entityClass, Class<S> targetClass) {
        return findAll(sort, offset, limit, targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Stream<T> findAll(Sort sort, long offset, long limit, Class<T> targetClass, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(targetClass, "Target class must not be null!");

        return findAllUsingQueryWithPostProcessing(setName, targetClass, sort, offset, limit,
            null, null);
    }

    public <T> boolean exists(Query query, Class<T> entityClass) {
        return exists(query, entityClass, getSetName(entityClass));
    }

    public <T> boolean exists(Query query, Class<T> entityClass, String setName) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return find(query, entityClass, setName).findAny().isPresent();
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
        Assert.notNull(entityClass, "Class must not be null!");
        return count(query, getSetName(entityClass));
    }

    @Override
    public long count(Query query, String setName) {
        Stream<KeyRecord> results = findAllRecordsUsingQuery(setName, query);
        return results.count();
    }

    @Override
    public <T> Stream<T> find(Query query, Class<T> entityClass) {
        return find(query, entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Stream<S> find(Query query, Class<T> entityClass, Class<S> targetClass) {
        return find(query, targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Stream<T> find(Query query, Class<T> targetClass, String setName) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(targetClass, "Target class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return findAllUsingQueryWithPostProcessing(setName, targetClass, query);
    }

    @Override
    public <T> Stream<T> findInRange(long offset, long limit, Sort sort,
                                     Class<T> entityClass) {
        return findInRange(offset, limit, sort, entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Stream<S> findInRange(long offset, long limit, Sort sort,
                                        Class<T> entityClass, Class<S> targetClass) {
        return findInRange(offset, limit, sort, targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Stream<T> findInRange(long offset, long limit, Sort sort,
                                     Class<T> targetClass, String setName) {
        Assert.notNull(targetClass, "Target class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return findAllUsingQueryWithPostProcessing(setName, targetClass, sort, offset, limit,
            null, null);
    }

    @Override
    public <T> long count(Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return count(getSetName(entityClass));
    }

    @Override
    public long count(String setName) {
        Assert.notNull(setName, "Set name must not be null!");

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
        return prepend(document, getSetName(document), fieldName, value);
    }

    @Override
    public <T> T prepend(T document, String setName, String fieldName, String value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(fieldName, "Field name must not be null!");

        try {
            AerospikeWriteData data = writeData(document, setName);
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
        return prepend(document, getSetName(document), values);
    }

    @Override
    public <T> T prepend(T document, String setName, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(values, "Values must not be null!");

        try {
            AerospikeWriteData data = writeData(document, setName);
            Operation[] ops = operations(values, Operation.Type.PREPEND, Operation.get());
            Record aeroRecord = this.client.operate(null, data.getKey(), ops);

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T append(T document, Map<String, String> values) {
        return append(document, getSetName(document), values);
    }

    @Override
    public <T> T append(T document, String setName, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(values, "Values must not be null!");

        try {
            AerospikeWriteData data = writeData(document, setName);
            Operation[] ops = operations(values, Operation.Type.APPEND, Operation.get());
            Record aeroRecord = this.client.operate(null, data.getKey(), ops);

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> T append(T document, String binName, String value) {
        return append(document, getSetName(document), binName, value);
    }

    @Override
    public <T> T append(T document, String setName, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");

        try {
            AerospikeWriteData data = writeData(document, setName);
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
        return add(document, getSetName(document), values);
    }

    @Override
    public <T> T add(T document, String setName, Map<String, Long> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(values, "Values must not be null!");

        try {
            AerospikeWriteData data = writeData(document, setName);
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
        return add(document, getSetName(document), binName, value);
    }

    @Override
    public <T> T add(T document, String setName, String binName, long value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");

        try {
            AerospikeWriteData data = writeData(document, setName);

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

    private void doPersistAndHandleError(AerospikeWriteData data, WritePolicy policy, Operation[] operations) {
        try {
            client.operate(policy, data.getKey(), operations);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private <T> void doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data, WritePolicy policy,
                                                           boolean firstlyDeleteBins) {
        try {
            Record newAeroRecord = putAndGetHeader(data, policy, firstlyDeleteBins);
            updateVersion(document, newAeroRecord);
        } catch (AerospikeException e) {
            throw translateCasError(e);
        }
    }

    private <T> void doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy policy) {
        try {
            Record newAeroRecord = putAndGetHeader(data, policy, false);
            updateVersion(document, newAeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private Record putAndGetHeader(AerospikeWriteData data, WritePolicy policy, boolean firstlyDeleteBins) {
        Key key = data.getKey();
        Operation[] operations = getPutAndGetHeaderOperations(data, firstlyDeleteBins);

        return client.operate(policy, key, operations);
    }

    private <T> Stream<T> findAllUsingQueryWithPostProcessing(String setName, Class<T> targetClass, Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        Stream<T> results = findAllUsingQueryWithDistinctPredicate(setName, targetClass,
            getDistinctPredicate(query), qualifier);
        return applyPostProcessingOnResults(results, query);
    }

    @SuppressWarnings("SameParameterValue")
    private <T> Stream<T> findAllUsingQueryWithPostProcessing(String setName, Class<T> targetClass, Sort sort,
                                                              long offset, long limit, Filter filter,
                                                              Qualifier qualifier) {
        verifyUnsortedWithOffset(sort, offset);
        Stream<T> results = findAllUsingQuery(targetClass, setName, filter, qualifier);
        return applyPostProcessingOnResults(results, sort, offset, limit);
    }

    @Override
    public <T> Stream<T> findAllUsingQuery(Class<T> entityClass, Filter filter,
                                           Qualifier qualifier) {
        return findAllUsingQuery(entityClass, getSetName(entityClass), filter, qualifier);
    }

    public <T, S> Stream<?> findAllUsingQuery(Class<T> entityClass, Class<S> targetClass, Filter filter,
                                              Qualifier qualifier) {
        return findAllRecordsUsingQuery(getSetName(entityClass), targetClass, filter, qualifier)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
    }

    @Override
    public <T> Stream<T> findAllUsingQuery(Class<T> targetClass, String setName, Filter filter,
                                           Qualifier qualifier) {
        return findAllRecordsUsingQuery(setName, targetClass, filter, qualifier)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
    }

    private <T> Stream<T> findAllUsingQueryWithDistinctPredicate(String setName, Class<T> targetClass,
                                                                 Predicate<KeyRecord> distinctPredicate,
                                                                 Qualifier... qualifiers) {
        return findAllRecordsUsingQuery(setName, targetClass, null, qualifiers)
            .filter(distinctPredicate)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
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

    private Stream<KeyRecord> findAllRecordsUsingQuery(String setName, Query query) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        return findAllRecordsUsingQuery(setName, null, null, qualifier);
    }

    private <T> Stream<KeyRecord> findAllRecordsUsingQuery(String setName, Class<T> targetClass, Filter filter,
                                                           Qualifier... qualifiers) {
        if (qualifiers != null && qualifiers.length > 0 && !allArrayElementsAreNull(qualifiers)) {
            validateQualifiers(qualifiers);

            Qualifier idQualifier = getOneIdQualifier(qualifiers);
            if (idQualifier != null) {
                // a special flow if there is id given
                return findByIdsInternalWithoutMapping(getIdValue(idQualifier), setName, targetClass,
                    excludeIdQualifier(qualifiers)).stream();
            }
        }

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
