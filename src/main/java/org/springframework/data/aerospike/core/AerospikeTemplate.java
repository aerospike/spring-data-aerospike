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
import org.springframework.data.aerospike.query.KeyRecordIterator;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.InfoCommandUtils;
import org.springframework.data.aerospike.util.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.util.StreamUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.DELETE_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.INSERT_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.SAVE_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.UPDATE_OPERATION;
import static org.springframework.data.aerospike.core.CoreUtils.getDistinctPredicate;
import static org.springframework.data.aerospike.core.CoreUtils.operations;
import static org.springframework.data.aerospike.core.CoreUtils.verifyUnsortedWithOffset;
import static org.springframework.data.aerospike.core.TemplateUtils.*;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;
import static org.springframework.data.aerospike.query.QualifierUtils.queryCriteriaIsNotNull;

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
    IndexesCacheRefresher<Integer> {

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
                             IndexRefresher indexRefresher,
                             ServerVersionSupport serverVersionSupport) {
        super(namespace, converter, mappingContext, exceptionTranslator, client.copyWritePolicyDefault(),
            serverVersionSupport);
        this.client = client;
        this.queryEngine = queryEngine;
        this.indexRefresher = indexRefresher;
    }

    @Override
    public IAerospikeClient getAerospikeClient() {
        return client;
    }

    @Override
    public long getQueryMaxRecords() {
        return queryEngine.getQueryMaxRecords();
    }

    @Override
    public Integer refreshIndexesCache() {
        return indexRefresher.refreshIndexes();
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
            WritePolicy writePolicy = expectGenerationCasAwarePolicy(data);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            doPersistWithVersionAndHandleCasError(document, data, writePolicy, true, SAVE_OPERATION);
        } else {
            WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.UPDATE);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
            doPersistAndHandleError(data, writePolicy, operations);
        }
    }

    @Override
    public <T> void saveAll(Iterable<T> documents) {
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for saving");
            return;
        }
        saveAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void saveAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for saving");
            return;
        }
        applyBufferedBatchWrite(documents, setName, SAVE_OPERATION);
    }

    private <T> void applyBufferedBatchWrite(Iterable<T> documents, String setName, OperationType operationType) {
        int batchSize = converter.getAerospikeDataSettings().getBatchWriteSize();
        List<T> docsList = new ArrayList<>();

        for (T doc : documents) {
            if (batchWriteSizeMatch(batchSize, docsList.size())) {
                batchWriteAllDocuments(docsList, setName, operationType);
                docsList.clear();
            }
            docsList.add(doc);
        }
        if (!docsList.isEmpty()) {
            batchWriteAllDocuments(docsList, setName, operationType);
        }
    }

    private <T> void batchWriteAllDocuments(List<T> documents, String setName, OperationType operationType) {
        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        switch (operationType) {
            case SAVE_OPERATION ->
                documents.forEach(document -> batchWriteDataList.add(getBatchWriteForSave(document, setName)));
            case INSERT_OPERATION ->
                documents.forEach(document -> batchWriteDataList.add(getBatchWriteForInsert(document, setName)));
            case UPDATE_OPERATION ->
                documents.forEach(document -> batchWriteDataList.add(getBatchWriteForUpdate(document, setName)));
            case DELETE_OPERATION ->
                documents.forEach(document -> batchWriteDataList.add(getBatchWriteForDelete(document, setName)));
            default -> throw new IllegalArgumentException("Unexpected operation name: " + operationType);
        }

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();
        try {
            BatchPolicy batchPolicy = (BatchPolicy) enrichPolicyWithTransaction(client, client.copyBatchPolicyDefault());
            client.operate(batchPolicy, batchWriteRecords);
        } catch (AerospikeException e) {
            throw translateError(e); // no exception is thrown for versions mismatch, only record's result code shows it
        }

        checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, operationType);
    }

    protected <T> void checkForErrorsAndUpdateVersion(List<BatchWriteData<T>> batchWriteDataList,
                                                      List<BatchRecord> batchWriteRecords,
                                                      OperationType operationType) {
        boolean errorsFound = false;
        String casErrorDocumentId = null;
        for (BaseAerospikeTemplate.BatchWriteData<T> data : batchWriteDataList) {
            if (!errorsFound && batchRecordFailed(data.batchRecord(), false)) {
                errorsFound = true;
            }
            if (data.hasVersionProperty()) {
                if (!batchRecordFailed(data.batchRecord(), false)) {
                    if (operationType != DELETE_OPERATION) updateVersion(data.document(), data.batchRecord().record);
                } else {
                    if (hasOptimisticLockingError(data.batchRecord().resultCode)) {
                        // ID can be a String or a primitive
                        casErrorDocumentId = data.batchRecord().key.userKey.toString();
                    }
                }
            }
        }

        if (errorsFound) {
            if (casErrorDocumentId != null) {
                throw getOptimisticLockingFailureException(
                    "Failed to %s the record with ID '%s' due to versions mismatch"
                        .formatted(operationType, casErrorDocumentId), null);
            }
            AerospikeException e = new AerospikeException("Errors during batch " + operationType);
            throw new AerospikeException.BatchRecordArray(batchWriteRecords.toArray(BatchRecord[]::new), e);
        }
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
        WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.CREATE_ONLY);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            // we are ignoring generation here as insert operation should fail with DuplicateKeyException if key
            // already exists
            // we do not mind which initial version is set in the document, BUT we need to update the version value
            // in the original document
            // also we do not want to handle aerospike error codes as cas aware error codes as we are ignoring
            // generation
            doPersistWithVersionAndHandleError(document, data, writePolicy);
        } else {
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            doPersistAndHandleError(data, writePolicy, operations);
        }
    }

    @Override
    public <T> void insertAll(Iterable<? extends T> documents) {
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for inserting");
            return;
        }
        insertAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void insertAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for inserting");
            return;
        }
        applyBufferedBatchWrite(documents, setName, INSERT_OPERATION);
    }

    @Override
    public <T> void persist(T document, WritePolicy writePolicy) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(writePolicy, "Policy must not be null!");
        persist(document, writePolicy, getSetName(document));
    }

    @Override
    public <T> void persist(T document, WritePolicy writePolicy, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(writePolicy, "Policy must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
        // not using initial writePolicy instance because it can get enriched with transaction id
        doPersistAndHandleError(data, new WritePolicy(writePolicy), operations);
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
            WritePolicy writePolicy = expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            doPersistWithVersionAndHandleCasError(document, data, writePolicy, true, UPDATE_OPERATION);
        } else {
            WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = Stream.concat(Stream.of(Operation.delete()), data.getBins().stream()
                .map(Operation::put)).toArray(Operation[]::new);
            doPersistAndHandleError(data, writePolicy, operations);
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
            WritePolicy writePolicy = expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            doPersistWithVersionAndHandleCasError(document, data, writePolicy, false, UPDATE_OPERATION);
        } else {
            WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            doPersistAndHandleError(data, writePolicy, operations);
        }
    }

    @Override
    public <T> void updateAll(Iterable<T> documents) {
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for updating");
            return;
        }
        updateAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void updateAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for updating");
            return;
        }
        applyBufferedBatchWrite(documents, setName, UPDATE_OPERATION);
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

        AerospikeWriteData data = writeData(document, setName);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            return doDeleteWithVersionAndHandleCasError(data);
        }
        return doDeleteIgnoreVersionAndTranslateError(data);
    }

    private boolean doDeleteWithVersionAndHandleCasError(AerospikeWriteData data) {
        try {
            WritePolicy writePolicy = expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);
            writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, writePolicy);
            return client.delete(writePolicy, data.getKey());
        } catch (AerospikeException e) {
            throw translateCasError(e, "Failed to delete record due to versions mismatch");
        }
    }

    private boolean doDeleteIgnoreVersionAndTranslateError(AerospikeWriteData data) {
        try {
            WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);
            writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, writePolicy);
            return client.delete(writePolicy, data.getKey());
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> boolean deleteById(Object id, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return deleteById(id, getSetName(entityClass));
    }

    @Override
    public boolean deleteById(Object id, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        try {
            Key key = getKey(id, setName);
            WritePolicy writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, ignoreGenerationPolicy());
            return client.delete(writePolicy, key);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    public <T> void delete(Query query, Class<T> entityClass, String setName) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<T> findQueryResults = find(query, entityClass, setName).filter(Objects::nonNull).toList();

        if (!findQueryResults.isEmpty()) {
            deleteAll(findQueryResults);
        }
    }

    @Override
    public <T> void delete(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Class must not be null!");

        delete(query, entityClass, getSetName(entityClass));
    }

    @Override
    public <T> void deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        deleteByIdsUsingQuery(ids, entityClass, getSetName(entityClass), query);
    }

    @Override
    public <T> void deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, String setName,
                                          @Nullable Query query) {
        List<Object> findQueryResults = findByIdsUsingQuery(ids, entityClass, entityClass, setName, query)
            .stream()
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

        if (!findQueryResults.isEmpty()) {
            deleteAll(findQueryResults);
        }
    }

    @Override
    public <T> void deleteAll(Iterable<T> documents) {
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for deleting");
            return;
        }

        String setName = getSetName(documents.iterator().next());
        int batchSize = converter.getAerospikeDataSettings().getBatchWriteSize();
        List<Object> documentsList = new ArrayList<>();
        for (Object document : documents) {
            if (batchWriteSizeMatch(batchSize, documentsList.size())) {
                deleteAll(documentsList, setName);
                documentsList.clear();
            }
            documentsList.add(document);
        }
        if (!documentsList.isEmpty()) {
            deleteAll(documentsList, setName);
        }
    }

    @Override
    public <T> void deleteAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(documents)) {
            logEmptyItems(log, "Documents for deleting");
            return;
        }
        applyBufferedBatchWrite(documents, setName, DELETE_OPERATION);
    }

    @Override
    public <T> void deleteByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        deleteByIds(ids, getSetName(entityClass));
    }

    @Override
    public <T> void deleteExistingByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        deleteExistingByIds(ids, getSetName(entityClass));
    }

    @Override
    public void deleteByIds(Iterable<?> ids, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }
        deleteByIds(ids, setName, false);
    }

    @Override
    public void deleteExistingByIds(Iterable<?> ids, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }
        deleteByIds(ids, setName, true);
    }

    private void deleteByIds(Iterable<?> ids, String setName, boolean skipNonExisting) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }

        int batchSize = converter.getAerospikeDataSettings().getBatchWriteSize();
        List<Object> idsList = new ArrayList<>();
        for (Object id : ids) {
            if (batchWriteSizeMatch(batchSize, idsList.size())) {
                doDeleteByIds(idsList, setName, skipNonExisting);
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            doDeleteByIds(idsList, setName, skipNonExisting);
        }
    }

    private void doDeleteByIds(Collection<?> ids, String setName, boolean skipNonExisting) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }

        Key[] keys = ids.stream()
            .map(id -> getKey(id, setName))
            .toArray(Key[]::new);

        // requires server ver. >= 6.0.0
        deleteAndHandleErrors(client, keys, skipNonExisting);
    }

    @Override
    public void deleteByIds(GroupedKeys groupedKeys) {
        if (areInvalidGroupedKeys(groupedKeys)) return;

        deleteGroupedEntitiesByGroupedKeys(groupedKeys);
    }

    private void deleteGroupedEntitiesByGroupedKeys(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));
        deleteAndHandleErrors(client, entitiesKeys.getKeys(), false);
    }

    @Override
    public <T> void deleteAll(Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        deleteAll(entityClass, null);
    }

    @Override
    public <T> void deleteAll(Class<T> entityClass, Instant beforeLastUpdate) {
        Assert.notNull(entityClass, "Class must not be null!");
        deleteAll(getSetName(entityClass), beforeLastUpdate);
    }

    @Override
    public void deleteAll(String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        deleteAll(setName, null);
    }

    @Override
    public void deleteAll(String setName, Instant beforeLastUpdate) {
        Assert.notNull(setName, "Set name must not be null!");
        Calendar beforeLastUpdateCalendar = convertToCalendar(beforeLastUpdate);

        try {
            client.truncate(null, getNamespace(), setName, beforeLastUpdateCalendar);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private void deleteAndHandleErrors(IAerospikeClient client, Key[] keys, boolean skipNonExisting) {
        BatchResults results;
        try {
            BatchPolicy batchPolicy = (BatchPolicy) enrichPolicyWithTransaction(client, client.copyBatchPolicyDefault());
            results = client.delete(batchPolicy, null, keys);
        } catch (AerospikeException e) {
            throw translateError(e);
        }

        if (results.records == null) {
            throw new AerospikeException.BatchRecordArray(results.records,
                new AerospikeException("Errors during batch delete"));
        }
        for (int i = 0; i < results.records.length; i++) {
            BatchRecord record = results.records[i];
            if (batchRecordFailed(record, skipNonExisting)) {
                throw new AerospikeException.BatchRecordArray(results.records,
                    new AerospikeException("Errors during batch delete"));
            }
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

            WritePolicy writePolicy = WritePolicyBuilder.builder(writePolicyDefault)
                .expiration(data.getExpiration())
                .build();
            writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, writePolicy);

            Record aeroRecord = client.operate(writePolicy, data.getKey(), ops);

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

            WritePolicy writePolicy = WritePolicyBuilder.builder(writePolicyDefault)
                .expiration(data.getExpiration())
                .build();
            writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, writePolicy);

            Record aeroRecord = client.operate(writePolicy, data.getKey(),
                Operation.add(new Bin(binName, value)), Operation.get());

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
            WritePolicy writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(), ops);

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
            WritePolicy writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(),
                Operation.append(new Bin(binName, value)),
                Operation.get(binName));

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
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
            WritePolicy writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(),
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
            WritePolicy writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(), ops);

            return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
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
        return (S) findByIdUsingQuery(id, entityClass, targetClass, setName, null);
    }

    private Record getRecord(AerospikePersistentEntity<?> entity, Key key, @Nullable Query query) {
        Record aeroRecord;
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), null, null);
        } else {
            Policy policy = enrichPolicyWithTransaction(client, getPolicyFilterExpOrDefault(client, queryEngine, query));
            aeroRecord = client.get(policy, key);
        }
        return aeroRecord;
    }

    private BatchPolicy getBatchPolicyFilterExp(Query query) {
        if (queryCriteriaIsNotNull(query)) {
            BatchPolicy batchPolicy = getAerospikeClient().copyBatchPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            batchPolicy.filterExp = queryEngine.getFilterExpressionsBuilder().build(qualifier);
            return batchPolicy;
        }
        return null;
    }

    private Key[] getKeys(Collection<?> ids, String setName) {
        return ids.stream()
            .map(id -> getKey(id, setName))
            .toArray(Key[]::new);
    }

    private <S> Object getRecordMapToTargetClass(AerospikePersistentEntity<?> entity, Key key, Class<S> targetClass,
                                                 @Nullable Query query) {
        Record aeroRecord;
        String[] binNames = getBinNamesFromTargetClass(targetClass, mappingContext);
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), binNames, query);
        } else {
            Policy policy = enrichPolicyWithTransaction(client, getPolicyFilterExpOrDefault(client, queryEngine, query));
            aeroRecord = client.get(policy, key, binNames);
        }
        return mapToEntity(key, targetClass, aeroRecord);
    }

    private Record getAndTouch(Key key, int expiration, String[] binNames, @Nullable Query query) {
        WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(client.copyWritePolicyDefault())
            .expiration(expiration);

        if (queryCriteriaIsNotNull(query)) {
            Qualifier qualifier = query.getCriteriaObject();
            writePolicyBuilder.filterExp(queryEngine.getFilterExpressionsBuilder().build(qualifier));
        }
        WritePolicy writePolicy = writePolicyBuilder.build();
        writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, writePolicy);

        try {
            if (binNames == null || binNames.length == 0) {
                return client.operate(writePolicy, key, Operation.touch(), Operation.get());
            } else {
                Operation[] operations = new Operation[binNames.length + 1];
                operations[0] = Operation.touch();

                for (int i = 1; i < operations.length; i++) {
                    operations[i] = Operation.get(binNames[i - 1]);
                }
                return client.operate(writePolicy, key, operations);
            }
        } catch (AerospikeException aerospikeException) {
            if (aerospikeException.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                return null;
            }
            throw aerospikeException;
        }
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

        int batchSize = converter.getAerospikeDataSettings().getBatchWriteSize();
        List<Object> idsList = new ArrayList<>();
        List<S> result = new ArrayList<>();

        for (Object id : ids) {
            if (batchWriteSizeMatch(batchSize, idsList.size())) {
                result = Stream.concat(
                    result.stream(),
                    ((List<S>) findByIdsUsingQuery(idsList, entityClass, targetClass, setName, null)).stream()
                ).toList();
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            result = Stream.concat(
                result.stream(),
                ((List<S>) findByIdsUsingQuery(idsList, entityClass, targetClass, setName, null)).stream()
            ).toList();
        }

        return result;
    }

    @Override
    public GroupedEntities findByIds(GroupedKeys groupedKeys) {
        if (areInvalidGroupedKeys(groupedKeys)) return GroupedEntities.builder().build();

        return findGroupedEntitiesByGroupedKeys(groupedKeys);
    }

    private GroupedEntities findGroupedEntitiesByGroupedKeys(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));
        BatchPolicy batchPolicy = (BatchPolicy) enrichPolicyWithTransaction(client, client.copyBatchPolicyDefault());
        Record[] aeroRecords = client.get(batchPolicy, entitiesKeys.getKeys());

        return toGroupedEntities(entitiesKeys, aeroRecords);
    }

    @Override
    public <T, S> Object findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass,
                                            Query query) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return findByIdUsingQuery(id, entityClass, targetClass, getSetName(entityClass), query);
    }

    @Override
    public <T, S> Object findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass, String setName,
                                            @Nullable Query query) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        try {
            AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
            Key key = getKey(id, setName);

            if (targetClass != null && targetClass != entityClass) {
                return getRecordMapToTargetClass(entity, key, targetClass, query);
            }
            return mapToEntity(key, entityClass, getRecord(entity, key, query));
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T, S> List<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                              Query query) {
        return findByIdsUsingQuery(ids, entityClass, targetClass, getSetName(entityClass), query);
    }

    @Override
    public <T, S> List<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                              String setName, Query query) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            Key[] keys = ids.stream()
                .map(id -> getKey(id, setName))
                .toArray(Key[]::new);

            BatchPolicy batchPolicy = (BatchPolicy) enrichPolicyWithTransaction(client, getBatchPolicyFilterExp(query));
            Class<?> target;
            Record[] aeroRecords;
            if (targetClass != null && targetClass != entityClass) {
                String[] binNames = getBinNamesFromTargetClass(targetClass, mappingContext);
                aeroRecords = client.get(batchPolicy, keys, binNames);
                target = targetClass;
            } else {
                aeroRecords = client.get(batchPolicy, keys);
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

    public IntStream findByIdsUsingQueryWithoutMapping(Collection<?> ids, String setName, Query query) {
        Assert.notNull(setName, "Set name must not be null!");

        try {
            Key[] keys;
            if (ids == null || ids.isEmpty()) {
                keys = new Key[0];
            } else {
                keys = ids.stream()
                    .map(id -> getKey(id, setName))
                    .toArray(Key[]::new);
            }

            BatchPolicy batchPolicy = getBatchPolicyFilterExp(query);

            Record[] aeroRecords;
            aeroRecords = getAerospikeClient().get(batchPolicy, keys);

            return IntStream.range(0, keys.length)
                .filter(index -> aeroRecords[index] != null);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
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

        return findWithPostProcessing(setName, targetClass, query);
    }

    private <T> Stream<T> find(Class<T> targetClass, String setName) {
        return findRecordsUsingQuery(setName, targetClass, null)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
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

        return find(targetClass, setName);
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

        return findWithPostProcessing(setName, targetClass, sort, offset, limit);
    }

    private <T> Stream<T> findWithPostProcessing(String setName, Class<T> targetClass, Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Stream<T> results = findUsingQueryWithDistinctPredicate(setName, targetClass,
            getDistinctPredicate(query), query);
        return applyPostProcessingOnResults(results, query);
    }

    @Override
    public <T, S> Stream<S> findUsingQueryWithoutPostProcessing(Class<T> entityClass, Class<S> targetClass,
                                                                Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        return findUsingQueryWithDistinctPredicate(getSetName(entityClass), targetClass,
            getDistinctPredicate(query), query);
    }

    private <T> Stream<T> findUsingQueryWithDistinctPredicate(String setName, Class<T> targetClass,
                                                              Predicate<KeyRecord> distinctPredicate,
                                                              Query query) {
        return findRecordsUsingQuery(setName, targetClass, query)
            .filter(distinctPredicate)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
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

        return findWithPostProcessing(setName, targetClass, sort, offset, limit);
    }

    @Override
    public <T> boolean exists(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return exists(id, getSetName(entityClass));
    }

    @Override
    public boolean exists(Object id, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        try {
            Key key = getKey(id, setName);

            WritePolicy writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, key, Operation.getHeader());
            return aeroRecord != null;
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> boolean exists(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Class must not be null!");
        return exists(query, getSetName(entityClass));
    }

    @Override
    public boolean exists(Query query, String setName) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(setName, "Set name must not be null!");

        return findKeyRecordsUsingQuery(setName, query).findAny().isPresent();
    }

    @Override
    public <T> boolean existsByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return existsByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public boolean existsByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        long findQueryResults = findByIdsUsingQueryWithoutMapping(ids, setName, query)
            .filter(Objects::nonNull)
            .count();
        return findQueryResults > 0;
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
            int replicationFactor = Utils.getReplicationFactor(client, nodes, namespace);

            long totalObjects = Arrays.stream(nodes)
                .mapToLong(node -> Utils.getObjectsCount(client, node, namespace, setName))
                .sum();

            return (nodes.length > 1) ? (totalObjects / replicationFactor) : totalObjects;
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
        return findKeyRecordsUsingQuery(setName, query).count();
    }

    private Stream<KeyRecord> findKeyRecordsUsingQuery(String setName, Query query) {
        Assert.notNull(setName, "Set name must not be null!");

        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query with id
                return findByIdsWithoutMapping(getIdValue(idQualifier), setName, null,
                    new Query(excludeIdQualifier(qualifier))).stream();
            }
        }

        KeyRecordIterator recIterator = queryEngine.selectForCount(namespace, setName, query);

        return StreamUtils.createStreamFromIterator(recIterator)
            .onClose(() -> {
                try {
                    recIterator.close();
                } catch (Exception e) {
                    log.error("Caught exception while closing query", e);
                }
            });
    }

    @Override
    public <T> long countByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return countByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public long countByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        return findByIdsUsingQueryWithoutMapping(ids, setName, query)
            .filter(Objects::nonNull)
            .count();
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
            resultSet = client.queryAggregate(null, statement, module,
                function, arguments.toArray(new Value[0]));
        else
            resultSet = client.queryAggregate(null, statement);
        return resultSet;
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
                if (!node.isActive()) continue;
                String response = InfoCommandUtils.request(client, node, "sindex-exists:ns=" + namespace +
                    ";indexname=" + indexName);
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

    private Record doPersistAndHandleError(AerospikeWriteData data, WritePolicy writePolicy, Operation[] operations) {
        try {
            writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, writePolicy);
            return client.operate(writePolicy, data.getKey(), operations);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private <T> void doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                           boolean firstlyDeleteBins, OperationType operationType) {
        try {
            Record newAeroRecord = putAndGetHeader(data, writePolicy, firstlyDeleteBins);
            updateVersion(document, newAeroRecord);
        } catch (AerospikeException e) {
            throw translateCasError(e, "Failed to " + operationType.toString() + " record due to versions mismatch");
        }
    }

    private <T> void doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy writePolicy) {
        try {
            Record newAeroRecord = putAndGetHeader(data, writePolicy, false);
            updateVersion(document, newAeroRecord);
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    private Record putAndGetHeader(AerospikeWriteData data, WritePolicy writePolicy, boolean firstlyDeleteBins) {
        Key key = data.getKey();
        Operation[] operations = getPutAndGetHeaderOperations(data, firstlyDeleteBins);
        writePolicy = (WritePolicy) enrichPolicyWithTransaction(client, writePolicy);

        return client.operate(writePolicy, key, operations);
    }

    @SuppressWarnings("SameParameterValue")
    private <T> Stream<T> findWithPostProcessing(String setName, Class<T> targetClass, Sort sort, long offset,
                                                 long limit) {
        verifyUnsortedWithOffset(sort, offset);
        Stream<T> results = find(targetClass, setName);
        return applyPostProcessingOnResults(results, sort, offset, limit);
    }

    private <T> Stream<T> applyPostProcessingOnResults(Stream<T> results, Query query) {
        if (query != null) {
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

    private <T> Stream<KeyRecord> findRecordsUsingQuery(String setName, Class<T> targetClass, Query query) {
        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query for id equality
                return findByIdsWithoutMapping(getIdValue(idQualifier), setName, targetClass,
                    new Query(excludeIdQualifier(qualifier))).stream();
            }
        }

        KeyRecordIterator recIterator;
        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass, mappingContext);
            recIterator = queryEngine.select(namespace, setName, binNames, query);
        } else {
            recIterator = queryEngine.select(namespace, setName, query);
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

    private List<KeyRecord> findByIdsWithoutMapping(Collection<?> ids, String setName,
                                                    Class<?> targetClass, Query query) {
        Assert.notNull(ids, "Ids must not be null");
        if (ids.isEmpty()) {
            return Collections.emptyList();
        }

        try {
            Key[] keys = getKeys(ids, setName);

            BatchPolicy batchPolicy = (BatchPolicy) enrichPolicyWithTransaction(client, getBatchPolicyFilterExp(query));
            Record[] aeroRecords;
            if (targetClass != null) {
                String[] binNames = getBinNamesFromTargetClass(targetClass, mappingContext);
                aeroRecords = client.get(batchPolicy, keys, binNames);
            } else {
                aeroRecords = client.get(batchPolicy, keys);
            }

            return IntStream.range(0, keys.length)
                .filter(index -> aeroRecords[index] != null)
                .mapToObj(index -> new KeyRecord(keys[index], aeroRecords[index]))
                .collect(Collectors.toList());
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }
}
