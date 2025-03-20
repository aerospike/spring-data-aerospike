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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.index.IndexesCacheRefresher;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.InfoCommandUtils;
import org.springframework.data.aerospike.util.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.aerospike.client.ResultCode.KEY_NOT_FOUND_ERROR;
import static java.util.Objects.nonNull;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.DELETE_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.INSERT_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.SAVE_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.UPDATE_OPERATION;
import static org.springframework.data.aerospike.core.CoreUtils.getDistinctPredicate;
import static org.springframework.data.aerospike.core.CoreUtils.operations;
import static org.springframework.data.aerospike.core.TemplateUtils.*;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;
import static org.springframework.data.aerospike.query.QualifierUtils.queryCriteriaIsNotNull;
import static org.springframework.data.aerospike.util.Utils.iterableToList;

/**
 * Primary implementation of {@link ReactiveAerospikeOperations}.
 *
 * @author Igor Ermolenko
 * @author Volodymyr Shpynta
 * @author Yevhen Tsyba
 */
@Slf4j
public class ReactiveAerospikeTemplate extends BaseAerospikeTemplate implements ReactiveAerospikeOperations,
    IndexesCacheRefresher<Mono<Integer>> {

    private static final Pattern INDEX_EXISTS_REGEX_PATTERN = Pattern.compile("^FAIL:(-?\\d+).*$");

    private final IAerospikeReactorClient reactorClient;
    private final ReactorQueryEngine reactorQueryEngine;
    private final ReactorIndexRefresher reactorIndexRefresher;

    public ReactiveAerospikeTemplate(IAerospikeReactorClient reactorClient,
                                     String namespace,
                                     MappingAerospikeConverter converter,
                                     AerospikeMappingContext mappingContext,
                                     AerospikeExceptionTranslator exceptionTranslator,
                                     ReactorQueryEngine queryEngine, ReactorIndexRefresher reactorIndexRefresher,
                                     ServerVersionSupport serverVersionSupport) {
        super(namespace, converter, mappingContext, exceptionTranslator,
            reactorClient.getAerospikeClient().copyWritePolicyDefault(), serverVersionSupport);
        Assert.notNull(reactorClient, "Aerospike reactor client must not be null!");
        this.reactorClient = reactorClient;
        this.reactorQueryEngine = queryEngine;
        this.reactorIndexRefresher = reactorIndexRefresher;
    }

    @Override
    public Mono<Integer> refreshIndexesCache() {
        return reactorIndexRefresher.refreshIndexes();
    }

    @Override
    public <T> Mono<T> save(T document) {
        Assert.notNull(document, "Document for saving must not be null!");
        return save(document, getSetName(document));
    }

    @Override
    public <T> Mono<T> save(T document, String setName) {
        Assert.notNull(document, "Document for saving must not be null!");
        AerospikeWriteData data = writeData(document, setName);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy = expectGenerationCasAwarePolicy(data);
            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));

            return doPersistWithVersionAndHandleCasError(document, data, writePolicy, operations, SAVE_OPERATION);
        } else {
            WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.UPDATE);
            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));

            return doPersistAndHandleError(document, data, writePolicy, operations);
        }
    }

    @Override
    public <T> Flux<T> saveAll(Iterable<T> documents) {
        validateForBatchWrite(documents, "Documents for saving");

        return saveAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> saveAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        validateForBatchWrite(documents, "Documents for saving");

        return applyBufferedBatchWrite(documents, setName, SAVE_OPERATION);
    }

    private <T> Flux<T> applyBufferedBatchWrite(Iterable<? extends T> documents, String setName,
                                                OperationType operationType) {
        return Flux.defer(() -> {
            int batchSize = converter.getAerospikeDataSettings().getBatchWriteSize();

            // Create batches
            return createNullTolerantBatches(documents, batchSize)
                .concatMap(batch -> batchWriteAllDocuments(batch, setName, operationType));
        });
    }

    private <T> Flux<T> batchWriteAllDocuments(List<T> documents, String setName, OperationType operationType) {
        return Flux.defer(() -> {
            try {
                List<BaseAerospikeTemplate.BatchWriteData<T>> batchWriteDataList = documents.stream().map(document ->
                    switch (operationType) {
                        case SAVE_OPERATION -> getBatchWriteForSave(document, setName);
                        case INSERT_OPERATION -> getBatchWriteForInsert(document, setName);
                        case UPDATE_OPERATION -> getBatchWriteForUpdate(document, setName);
                        case DELETE_OPERATION -> getBatchWriteForDelete(document, setName);
                    }
                ).toList();

                List<BatchRecord> batchWriteRecords = batchWriteDataList.stream()
                    .map(BatchWriteData::batchRecord)
                    .toList();

                return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                    .copyBatchPolicyDefault())
                    .flatMapMany(batchPolicyEnriched ->
                        batchWriteAndCheckForErrors((BatchPolicy) batchPolicyEnriched, batchWriteRecords,
                            batchWriteDataList,
                            operationType));
            } catch (Exception e) {
                return Flux.error(e);
            }
        });
    }

    private <T> Flux<T> batchWriteAndCheckForErrors(BatchPolicy batchPolicy, List<BatchRecord> batchWriteRecords,
                                                    List<BatchWriteData<T>> batchWriteDataList,
                                                    OperationType operationType) {
        return reactorClient
            .operate(batchPolicy, batchWriteRecords)
            .onErrorMap(this::translateError)
            .flatMap(ignore -> checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, operationType))
            .flux()
            .flatMapIterable(list -> list.stream().map(BatchWriteData::document).toList());
    }

    private <T> Mono<List<BatchWriteData<T>>> checkForErrorsAndUpdateVersion(List<BatchWriteData<T>> batchWriteDataList,
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
                return Mono.error(getOptimisticLockingFailureException(
                    "Failed to %s the record with ID '%s' due to versions mismatch"
                        .formatted(operationType, casErrorDocumentId), null));
            }
            AerospikeException e = new AerospikeException("Errors during batch " + operationType);
            return Mono.error(
                new AerospikeException.BatchRecordArray(batchWriteRecords.toArray(BatchRecord[]::new), e));
        }

        return Mono.just(batchWriteDataList);
    }

    @Override
    public <T> Mono<T> insert(T document) {
        return insert(document, getSetName(document));
    }

    @Override
    public <T> Mono<T> insert(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.CREATE_ONLY);

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            // we are ignoring generation here as insert operation should fail with DuplicateKeyException if key
            // already exists,
            // and we do not mind which initial version is set in the document, BUT we need to update the version
            // value in the original document
            // also we do not want to handle aerospike error codes as cas aware error codes as we are ignoring
            // generation
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put, null,
                Operation.array(Operation.getHeader()));
            return doPersistWithVersionAndHandleError(document, data, writePolicy, operations);
        } else {
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            return doPersistAndHandleError(document, data, writePolicy, operations);
        }
    }

    @Override
    public <T> Flux<T> insertAll(Iterable<? extends T> documents) {
        validateForBatchWrite(documents, "Documents for insert");

        return insertAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> insertAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        validateForBatchWrite(documents, "Documents for insert");

        return applyBufferedBatchWrite(documents, setName, INSERT_OPERATION);
    }

    @Override
    public <T> Mono<T> persist(T document, WritePolicy writePolicy) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(writePolicy, "Policy must not be null!");
        return persist(document, writePolicy, getSetName(document));
    }

    @Override
    public <T> Mono<T> persist(T document, WritePolicy writePolicy, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(writePolicy, "Policy must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
        // not using initial writePolicy instance because it can get enriched with transaction id
        return enrichPolicyWithTransaction(reactorClient, new WritePolicy(writePolicy))
            .flatMap(writePolicyEnriched ->
                doPersistAndHandleError(document, data, (WritePolicy) writePolicyEnriched, operations));
    }

    @Override
    public <T> Mono<T> update(T document) {
        return update(document, getSetName(document));
    }

    @Override
    public <T> Mono<T> update(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy = expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()), Operation.array(Operation.getHeader()));
            return doPersistWithVersionAndHandleCasError(document, data, writePolicy, operations, UPDATE_OPERATION);
        } else {
            WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
            return doPersistAndHandleError(document, data, writePolicy, operations);
        }
    }

    @Override
    public <T> Mono<T> update(T document, Collection<String> fields) {
        return update(document, getSetName(document), fields);
    }

    @Override
    public <T> Mono<T> update(T document, String setName, Collection<String> fields) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(fields, "Fields must not be null!");

        AerospikeWriteData data = writeDataWithSpecificFields(document, setName, fields);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy = expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put, null,
                Operation.array(Operation.getHeader()));
            return doPersistWithVersionAndHandleCasError(document, data, writePolicy, operations, UPDATE_OPERATION);
        } else {
            WritePolicy writePolicy = ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            return doPersistAndHandleError(document, data, writePolicy, operations);
        }
    }

    @Override
    public <T> Flux<T> updateAll(Iterable<? extends T> documents) {
        validateForBatchWrite(documents, "Documents for update");

        return updateAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> updateAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        validateForBatchWrite(documents, "Documents for update");

        return applyBufferedBatchWrite(documents, setName, UPDATE_OPERATION);
    }

    @Override
    public <T> Mono<Boolean> delete(T document) {
        return delete(document, getSetName(document));
    }

    @Override
    public <T> Mono<Boolean> delete(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(document, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            return enrichPolicyWithTransaction(reactorClient, expectGenerationPolicy(data))
                .flatMap(writePolicyEnriched -> reactorClient.delete((WritePolicy) writePolicyEnriched, data.getKey()))
                .hasElement()
                .onErrorMap(e -> translateCasThrowable(e, DELETE_OPERATION.toString()));
        }
        return enrichPolicyWithTransaction(reactorClient, ignoreGenerationPolicy())
            .flatMap(writePolicyEnriched -> reactorClient.delete((WritePolicy) writePolicyEnriched, data.getKey()))
            .hasElement()
            .onErrorMap(this::translateError);
    }

    public <T> Mono<Void> delete(Query query, Class<T> entityClass, String setName) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        Mono<List<T>> findQueryResults = find(query, entityClass, setName)
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

        return findQueryResults.flatMap(list -> {
                if (!list.isEmpty()) {
                    return deleteAll(list);
                }
                return Mono.empty();
            }
        );
    }

    @Override
    public <T> Mono<Void> delete(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Class must not be null!");

        return delete(query, entityClass, getSetName(entityClass));
    }

    @Override
    public <T> Mono<Void> deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return deleteByIdsUsingQuery(ids, entityClass, getSetName(entityClass), query);
    }

    @Override
    public <T> Mono<Void> deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, String setName,
                                                @Nullable Query query) {
        Mono<List<Object>> findQueryResults = findByIdsUsingQuery(ids, entityClass, entityClass, setName, query)
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

        return findQueryResults.flatMap(list -> {
                if (!list.isEmpty()) {
                    return deleteAll(list);
                }
                return Mono.empty();
            }
        );
    }

    @Override
    public <T> Mono<Boolean> deleteById(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");

        return deleteById(id, getSetName(entityClass));
    }

    @Override
    public Mono<Boolean> deleteById(Object id, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return enrichPolicyWithTransaction(reactorClient, ignoreGenerationPolicy())
            .flatMap(writePolicyEnriched ->
                reactorClient.delete((WritePolicy) writePolicyEnriched, getKey(id, setName)))
            .map(k -> true)
            .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Void> deleteAll(Iterable<T> documents) {
        validateForBatchWrite(documents, "Documents for deleting");

        return deleteAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Mono<Void> deleteAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        validateForBatchWrite(documents, "Documents for deleting");

        return applyBufferedBatchWrite(documents, setName, DELETE_OPERATION).then();
    }

    @Override
    public <T> Mono<Void> deleteByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        validateForBatchWrite(ids, "IDs");

        return deleteByIds(ids, getSetName(entityClass));
    }

    @Override
    public <T> Mono<Void> deleteExistingByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        validateForBatchWrite(ids, "IDs");

        return deleteExistingByIds(ids, getSetName(entityClass));
    }

    @Override
    public Mono<Void> deleteByIds(Iterable<?> ids, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        validateForBatchWrite(ids, "IDs");

        List<Object> idsList = new ArrayList<>();
        Flux<Void> result = Flux.empty();
        for (Object id : ids) {
            if (batchWriteSizeMatch(converter.getAerospikeDataSettings().getBatchWriteSize(), idsList.size())) {
                result = deleteByIds(idsList, setName, false).concatWith(result);
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            result = deleteByIds(idsList, setName, false).concatWith(result);
        }

        return result.then();
    }

    @Override
    public Mono<Void> deleteExistingByIds(Iterable<?> ids, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        validateForBatchWrite(ids, "IDs");

        List<Object> idsList = new ArrayList<>();
        Flux<Void> result = Flux.empty();
        for (Object id : ids) {
            if (batchWriteSizeMatch(converter.getAerospikeDataSettings().getBatchWriteSize(), idsList.size())) {
                result = deleteByIds(idsList, setName, true).concatWith(result);
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            result = deleteByIds(idsList, setName, true).concatWith(result);
        }

        return result.then();
    }

    private Mono<Void> deleteByIds(Collection<?> ids, String setName, boolean skipNonExisting) {
        Assert.notNull(setName, "Set name must not be null!");
        validateForBatchWrite(ids, "IDs");

        Key[] keys = ids.stream()
            .map(id -> getKey(id, setName))
            .toArray(Key[]::new);

        return batchDeleteAndCheckForErrors(reactorClient, keys, skipNonExisting);
    }

    private Mono<Void> batchDeleteAndCheckForErrors(IAerospikeReactorClient reactorClient, Key[] keys,
                                                    boolean skipNonExisting)
    {
        Function<BatchResults, Mono<Void>> checkForErrors = results -> {
            if (results.records == null) {
                return Mono.error(new AerospikeException.BatchRecordArray(results.records,
                    new AerospikeException("Errors during batch delete")));
            }
            for (BatchRecord record : results.records) {
                if (batchRecordFailed(record, skipNonExisting)) {
                    return Mono.error(new AerospikeException.BatchRecordArray(results.records,
                        new AerospikeException("Errors during batch delete")));
                }
            }
            return Mono.empty();
        };

        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.delete((BatchPolicy) batchPolicy, null, keys))
            .onErrorMap(this::translateError)
            .flatMap(checkForErrors);
    }

    @Override
    public Mono<Void> deleteByIds(GroupedKeys groupedKeys) {
        validateGroupedKeys(groupedKeys);

        if (groupedKeys.getEntitiesKeys().isEmpty()) {
            return Mono.empty();
        }

        return deleteEntitiesByGroupedKeys(groupedKeys);
    }

    private Mono<Void> deleteEntitiesByGroupedKeys(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));

        enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.delete((BatchPolicy) batchPolicy, null, entitiesKeys.getKeys()))
            .doOnError(this::translateError);

        return batchDeleteAndCheckForErrors(reactorClient, entitiesKeys.getKeys(), false);
    }

    @Override
    public <T> Mono<Void> deleteAll(Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");

        return deleteAll(getSetName(entityClass), null);
    }

    @Override
    public <T> Mono<Void> deleteAll(Class<T> entityClass, Instant beforeLastUpdate) {
        Assert.notNull(entityClass, "Class must not be null!");

        return deleteAll(getSetName(entityClass), beforeLastUpdate);
    }

    @Override
    public Mono<Void> deleteAll(String setName) {
        Assert.notNull(setName, "Set name must not be null!");

        return deleteAll(setName, null);
    }

    @Override
    public Mono<Void> deleteAll(String setName, Instant beforeLastUpdate) {
        Assert.notNull(setName, "Set name must not be null!");
        Calendar beforeLastUpdateCalendar = convertToCalendar(beforeLastUpdate);

        try {
            return Mono.fromRunnable(
                () -> reactorClient.getAerospikeClient().truncate(null, namespace, setName, beforeLastUpdateCalendar));
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> Mono<T> add(T document, Map<String, Long> values) {
        return add(document, getSetName(document), values);
    }

    @Override
    public <T> Mono<T> add(T document, String setName, Map<String, Long> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(values, "Values must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        Operation[] operations = new Operation[values.size() + 1];
        int x = 0;
        for (Map.Entry<String, Long> entry : values.entrySet()) {
            operations[x] = new Operation(Operation.Type.ADD, entry.getKey(), Value.get(entry.getValue()));
            x++;
        }
        operations[x] = Operation.get();

        WritePolicy writePolicy = WritePolicyBuilder.builder(writePolicyDefault)
            .expiration(data.getExpiration())
            .build();

        return enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                executeOperationsOnValue(document, data, (WritePolicy) writePolicyEnriched, operations));
    }

    @Override
    public <T> Mono<T> add(T document, String binName, long value) {
        return add(document, getSetName(document), binName, value);
    }

    @Override
    public <T> Mono<T> add(T document, String setName, String binName, long value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        WritePolicy writePolicy = WritePolicyBuilder.builder(writePolicyDefault)
            .expiration(data.getExpiration())
            .build();

        Operation[] operations = {Operation.add(new Bin(binName, value)), Operation.get(binName)};
        return enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                executeOperationsOnValue(document, data, (WritePolicy) writePolicyEnriched, operations));
    }

    @Override
    public <T> Mono<T> append(T document, Map<String, String> values) {
        return append(document, getSetName(document), values);
    }

    @Override
    public <T> Mono<T> append(T document, String setName, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(values, "Values must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        Operation[] operations = operations(values, Operation.Type.APPEND, Operation.get());
        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyWritePolicyDefault())
            .flatMap(writePolicyEnriched ->
                executeOperationsOnValue(document, data, (WritePolicy) writePolicyEnriched, operations));
    }

    @Override
    public <T> Mono<T> append(T document, String binName, String value) {
        return append(document, getSetName(document), binName, value);
    }

    @Override
    public <T> Mono<T> append(T document, String setName, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        Operation[] operations = {Operation.append(new Bin(binName, value)), Operation.get(binName)};
        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyWritePolicyDefault())
            .flatMap(writePolicyEnriched ->
                executeOperationsOnValue(document, data, (WritePolicy) writePolicyEnriched, operations));
    }

    @Override
    public <T> Mono<T> prepend(T document, Map<String, String> values) {
        return prepend(document, getSetName(document), values);
    }

    @Override
    public <T> Mono<T> prepend(T document, String setName, Map<String, String> values) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(values, "Values must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        Operation[] operations = operations(values, Operation.Type.PREPEND, Operation.get());
        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyWritePolicyDefault())
            .flatMap(writePolicyEnriched -> executeOperationsOnValue(document, data,
                (WritePolicy) writePolicyEnriched, operations));
    }

    @Override
    public <T> Mono<T> prepend(T document, String binName, String value) {
        return prepend(document, getSetName(document), binName, value);
    }

    @Override
    public <T> Mono<T> prepend(T document, String setName, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);
        Operation[] operations = {Operation.prepend(new Bin(binName, value)), Operation.get(binName)};
        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyWritePolicyDefault())
            .flatMap(writePolicyEnriched ->
                executeOperationsOnValue(document, data, (WritePolicy) writePolicyEnriched, operations));
    }

    private <T> Mono<T> executeOperationsOnValue(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                 Operation[] operations) {
        return reactorClient.operate(writePolicy, data.getKey(), operations)
            .filter(keyRecord -> Objects.nonNull(keyRecord.record))
            .map(keyRecord -> mapToEntity(keyRecord.key, getEntityClass(document), keyRecord.record))
            .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<T> execute(Supplier<T> supplier) {
        Assert.notNull(supplier, "Supplier must not be null!");

        return Mono.fromSupplier(supplier)
            .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<T> findById(Object id, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return findById(id, entityClass, getSetName(entityClass));
    }

    @Override
    public <T> Mono<T> findById(Object id, Class<T> entityClass, String setName) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        Key key = getKey(id, setName);

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(),
                "Touch on read is not supported for entity without expiration property");
            return getAndTouch(key, entity.getExpiration(), null, null)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(this::translateError);
        } else {
            return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                .copyReadPolicyDefault())
                .flatMap(policy -> reactorClient.get(policy, key))
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record))
                .onErrorMap(this::translateError);
        }
    }

    @Override
    public <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass) {
        return findById(id, entityClass, targetClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass, String setName) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        Key key = getKey(id, setName);

        String[] binNames = getBinNamesFromTargetClass(targetClass);

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(),
                "Touch on read is not supported for entity without expiration property");
            return getAndTouch(key, entity.getExpiration(), binNames, null)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, targetClass, keyRecord.record))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(this::translateError);
        } else {
            return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                .copyReadPolicyDefault())
                .flatMap(policy -> reactorClient.get(policy, key, binNames))
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, targetClass, keyRecord.record))
                .onErrorMap(this::translateError);
        }
    }

    @Override
    public <T> Flux<T> findByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return findByIds(ids, entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Flux<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return findByIds(ids, targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Flux<T> findByIds(Iterable<?> ids, Class<T> targetClass, String setName) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(targetClass, "Class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<Object> idsList = new ArrayList<>();
        Flux<T> result = Flux.empty();
        for (Object id : ids) {
            if (batchWriteSizeMatch(converter.getAerospikeDataSettings().getBatchWriteSize(), idsList.size())) {
                result = Flux.concat(result, findByIds(idsList, targetClass, setName));
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            result = Flux.concat(result, findByIds(idsList, targetClass, setName));
        }
        return result;
    }

    private <T> Flux<T> findByIds(Collection<?> ids, Class<T> targetClass, String setName) {
        Key[] keys = iterableToList(ids).stream()
            .map(id -> getKey(id, setName))
            .toArray(Key[]::new);

        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.get((BatchPolicy) batchPolicy, keys))
            .flatMap(kr -> Mono.just(kr.asMap()))
            .flatMapMany(keyRecordMap -> {
                List<T> entities = keyRecordMap.entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .map(entry -> mapToEntity(entry.getKey(), targetClass, entry.getValue()))
                    .collect(Collectors.toList());
                return Flux.fromIterable(entities);
            });
    }

    @Override
    public Mono<GroupedEntities> findByIds(GroupedKeys groupedKeys) {
        validateGroupedKeys(groupedKeys);

        if (groupedKeys.getEntitiesKeys().isEmpty()) {
            return Mono.just(GroupedEntities.builder().build());
        }

        return findGroupedEntitiesByGroupedKeys(reactorClient.getAerospikeClient()
            .copyBatchPolicyDefault(), groupedKeys);
    }

    private Mono<GroupedEntities> findGroupedEntitiesByGroupedKeys(BatchPolicy batchPolicy, GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));

        return enrichPolicyWithTransaction(reactorClient, batchPolicy)
            .flatMap(batchPolicyEnriched -> reactorClient.get((BatchPolicy) batchPolicyEnriched,
                entitiesKeys.getKeys()))
            .map(item -> toGroupedEntities(entitiesKeys, item.records))
            .onErrorMap(this::translateError);
    }

    @Override
    public <T, S> Mono<?> findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass,
                                             Query query) {
        return findByIdUsingQuery(id, entityClass, targetClass, getSetName(entityClass), query);
    }

    @Override
    public <T, S> Mono<?> findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass, String setName,
                                             Query query) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        Key key = getKey(id, setName);

        String[] binNames = getBinNamesFromTargetClass(targetClass);

        final Class<?> target;
        if (targetClass != null && targetClass != entityClass) {
            target = targetClass;
        } else {
            target = entityClass;
        }

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(),
                "Touch on read is not supported for entity without expiration property");
            return getAndTouch(key, entity.getExpiration(), binNames, query)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(this::translateError);
        } else {
            Policy policy = null;
            if (queryCriteriaIsNotNull(query)) {
                policy = reactorClient.getAerospikeClient().copyReadPolicyDefault();
                Qualifier qualifier = query.getCriteriaObject();
                policy.filterExp = reactorQueryEngine.getFilterExpressionsBuilder().build(qualifier);
            }
            return enrichPolicyWithTransaction(reactorClient, policy)
                .flatMap(rPolicy -> reactorClient.get(rPolicy, key, binNames))
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record))
                .onErrorMap(this::translateError);
        }
    }

    @Override
    public <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                              @Nullable Query query) {
        return findByIdsUsingQuery(ids, entityClass, targetClass, getSetName(entityClass), query);
    }

    @Override
    public <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                              String setName, @Nullable Query query) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }

        BatchPolicy batchPolicy = getBatchPolicyFilterExp(query);

        Class<?> target;
        if (targetClass != null && targetClass != entityClass) {
            target = targetClass;
        } else {
            target = entityClass;
        }

        Flux<?> results = Flux.fromIterable(ids)
            .map(id -> getKey(id, setName))
            .flatMap(key -> getFromClient(batchPolicy, key, targetClass))
            .filter(keyRecord -> nonNull(keyRecord.record))
            .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record));

        return applyPostProcessingOnResults(results, query);
    }

    private Flux<?> findByIdsUsingQueryWithoutMapping(Collection<?> ids, String setName, Query query) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }

        BatchPolicy batchPolicy = getBatchPolicyFilterExp(query);

        return Flux.fromIterable(ids)
            .map(id -> getKey(id, setName))
            .flatMap(key -> getFromClient(batchPolicy, key, null))
            .filter(keyRecord -> nonNull(keyRecord.record));
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return find(query, entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Flux<S> find(Query query, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return find(query, targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> targetClass, String setName) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(targetClass, "Target class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return findWithPostProcessing(setName, targetClass, query);
    }

    @Override
    public <T> Flux<T> findAll(Class<T> entityClass) {
        Assert.notNull(entityClass, "Entity class must not be null!");

        return findAll(entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Flux<S> findAll(Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(targetClass, "Target class must not be null!");

        return findAll(targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Flux<T> findAll(Class<T> targetClass, String setName) {
        Assert.notNull(targetClass, "Target class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return find(setName, targetClass);
    }

    @Override
    public <T> Flux<T> findAll(Sort sort, long offset, long limit, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");

        return findAll(sort, offset, limit, entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Flux<S> findAll(Sort sort, long offset, long limit, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        Assert.notNull(targetClass, "Target class must not be null!");

        return findAll(sort, offset, limit, targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Flux<T> findAll(Sort sort, long offset, long limit, Class<T> targetClass, String setName) {
        Assert.notNull(targetClass, "Target class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return findWithPostProcessing(setName, targetClass, sort, offset, limit);
    }

    @Override
    public <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");

        return findInRange(offset, limit, sort, entityClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Flux<S> findInRange(long offset, long limit, Sort sort, Class<T> entityClass, Class<S> targetClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        Assert.notNull(targetClass, "Target class must not be null!");

        return findInRange(offset, limit, sort, targetClass, getSetName(entityClass));
    }

    @Override
    public <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> targetClass, String setName) {
        Assert.notNull(targetClass, "Target Class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return findWithPostProcessing(setName, targetClass, sort, offset, limit);
    }

    private BatchPolicy getBatchPolicyFilterExp(Query query) {
        if (queryCriteriaIsNotNull(query)) {
            BatchPolicy batchPolicy = reactorClient.getAerospikeClient().copyBatchPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            batchPolicy.filterExp = reactorQueryEngine.getFilterExpressionsBuilder().build(qualifier);
            return batchPolicy;
        }
        return null;
    }

    private Mono<KeyRecord> getFromClient(BatchPolicy batchPolicy, Key key, Class<?> targetClass) {
        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass);
            return enrichPolicyWithTransaction(reactorClient, batchPolicy)
                .flatMap(rPolicy -> reactorClient.get(rPolicy, key, binNames));
        } else {
            return enrichPolicyWithTransaction(reactorClient, batchPolicy)
                .flatMap(rPolicy -> reactorClient.get(rPolicy, key));
        }
    }

    @Override
    public <T> Mono<Boolean> exists(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");
        return exists(id, getSetName(entityClass));
    }

    @Override
    public Mono<Boolean> exists(Object id, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        Key key = getKey(id, setName);
        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyReadPolicyDefault())
            .flatMap(policy -> reactorClient.exists(policy, key))
            .map(Objects::nonNull)
            .defaultIfEmpty(false)
            .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Boolean> exists(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Class must not be null!");
        return exists(query, getSetName(entityClass));
    }

    @Override
    public Mono<Boolean> exists(Query query, String setName) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(setName, "Set name must not be null!");
        return findKeyRecordsUsingQuery(setName, query).hasElements();
    }

    @Override
    public <T> Mono<Boolean> existsByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return existsByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public Mono<Boolean> existsByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        return findByIdsUsingQueryWithoutMapping(ids, setName, query)
            .filter(Objects::nonNull)
            .hasElements();
    }

    @Override
    public <T> Mono<Long> count(Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        String setName = getSetName(entityClass);
        return count(setName);
    }

    @Override
    public Mono<Long> count(String setName) {
        Assert.notNull(setName, "Set name must not be null!");

        try {
            return Mono.fromCallable(() -> countSet(setName));
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Override
    public <T> Mono<Long> countByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return countByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public Mono<Long> countByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        return findByIdsUsingQueryWithoutMapping(ids, setName, query)
            .filter(Objects::nonNull)
            .count();
    }

    private long countSet(String setName) {
        Node[] nodes = reactorClient.getAerospikeClient().getNodes();

        int replicationFactor = Utils.getReplicationFactor(reactorClient.getAerospikeClient(), nodes, namespace);

        long totalObjects = Arrays.stream(nodes)
            .mapToLong(node -> Utils.getObjectsCount(reactorClient.getAerospikeClient(), node, namespace, setName))
            .sum();

        return (nodes.length > 1) ? (totalObjects / replicationFactor) : totalObjects;
    }

    @Override
    public <T> Mono<Long> count(Query query, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");

        return count(query, getSetName(entityClass));
    }

    @Override
    public Mono<Long> count(Query query, String setName) {
        Assert.notNull(setName, "Set for count must not be null!");

        return findKeyRecordsUsingQuery(setName, query).count();
    }

    private Flux<KeyRecord> findKeyRecordsUsingQuery(String setName, Query query) {
        Assert.notNull(setName, "Set name must not be null!");

        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query with id
                return findByIdsWithoutMapping(getIdValue(idQualifier), setName, null,
                    new Query(excludeIdQualifier(qualifier)));
            }
        }
        return reactorQueryEngine.selectForCount(namespace, setName, query);
    }

    @Override
    public <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                                      String binName, IndexType indexType) {
        return createIndex(entityClass, indexName, binName, indexType, IndexCollectionType.DEFAULT);
    }

    @Override
    public <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                                      String binName, IndexType indexType, IndexCollectionType indexCollectionType) {
        return createIndex(entityClass, indexName, binName, indexType, indexCollectionType, new CTX[0]);
    }

    @Override
    public <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                                      String binName, IndexType indexType, IndexCollectionType indexCollectionType,
                                      CTX... ctx) {
        return createIndex(getSetName(entityClass), indexName, binName, indexType, indexCollectionType, ctx);
    }

    @Override
    public Mono<Void> createIndex(String setName, String indexName,
                                  String binName, IndexType indexType) {
        return createIndex(setName, indexName, binName, indexType, IndexCollectionType.DEFAULT);
    }

    @Override
    public Mono<Void> createIndex(String setName, String indexName,
                                  String binName, IndexType indexType, IndexCollectionType indexCollectionType) {
        return createIndex(setName, indexName, binName, indexType, indexCollectionType, new CTX[0]);
    }

    @Override
    public Mono<Void> createIndex(String setName, String indexName,
                                  String binName, IndexType indexType, IndexCollectionType indexCollectionType,
                                  CTX... ctx) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");
        Assert.notNull(binName, "Bin name must not be null!");
        Assert.notNull(indexType, "Index type must not be null!");
        Assert.notNull(indexCollectionType, "Index collection type must not be null!");
        Assert.notNull(ctx, "Ctx must not be null!");

        return reactorClient.createIndex(null, namespace,
                setName, indexName, binName, indexType, indexCollectionType, ctx)
            .then(refreshIndexesCache())
            .then()
            .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Void> deleteIndex(Class<T> entityClass, String indexName) {
        Assert.notNull(entityClass, "Class must not be null!");
        return deleteIndex(getSetName(entityClass), indexName);
    }

    @Override
    public Mono<Void> deleteIndex(String setName, String indexName) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(indexName, "Index name must not be null!");

        return reactorClient.dropIndex(null, namespace, setName, indexName)
            .then(refreshIndexesCache())
            .then()
            .onErrorMap(this::translateError);
    }

    @Override
    public Mono<Boolean> indexExists(String indexName) {
        Assert.notNull(indexName, "Index name must not be null!");

        try {
            Node[] nodes = reactorClient.getAerospikeClient().getNodes();
            for (Node node : nodes) {
                if (!node.isActive()) continue;
                String response = InfoCommandUtils.request(reactorClient.getAerospikeClient(), node,
                    "sindex-exists:ns=" + namespace + ";indexname=" + indexName);
                if (response == null) throw new AerospikeException("Null node response");

                if (response.equalsIgnoreCase("true")) {
                    return Mono.just(true);
                } else if (response.equalsIgnoreCase("false")) {
                    return Mono.just(false);
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
        return Mono.just(false);
    }

    @Override
    public IAerospikeReactorClient getAerospikeReactorClient() {
        return reactorClient;
    }

    @Override
    public long getQueryMaxRecords() {
        return reactorQueryEngine.getQueryMaxRecords();
    }

    private <T> Mono<T> doPersistAndHandleError(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                Operation[] operations) {
        return enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                reactorClient.operate((WritePolicy) writePolicyEnriched, data.getKey(), operations))
            .map(docKey -> document)
            .onErrorMap(this::translateError);
    }

    private <T> Mono<T> doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data,
                                                              WritePolicy writePolicy, Operation[] operations,
                                                              OperationType operationType) {
        return enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched -> putAndGetHeader(data, (WritePolicy) writePolicyEnriched, operations))
            .map(newRecord -> updateVersion(document, newRecord))
            .onErrorMap(AerospikeException.class, i -> translateCasError(i,
                "Failed to " + operationType.toString() + " record due to versions mismatch"));
    }

    private <T> Mono<T> doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                           Operation[] operations) {
        return enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched -> putAndGetHeader(data, (WritePolicy) writePolicyEnriched, operations))
            .map(newRecord -> updateVersion(document, newRecord))
            .onErrorMap(AerospikeException.class, this::translateError);
    }

    private Mono<Record> putAndGetHeader(AerospikeWriteData data, WritePolicy writePolicy, Operation[] operations) {
        return reactorClient.operate(writePolicy, data.getKey(), operations)
            .map(keyRecord -> keyRecord.record);
    }

    private Mono<KeyRecord> getAndTouch(Key key, int expiration, String[] binNames, Query query) {
        WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(writePolicyDefault)
            .expiration(expiration);

        if (queryCriteriaIsNotNull(query)) {
            Qualifier qualifier = query.getCriteriaObject();
            writePolicyBuilder.filterExp(reactorQueryEngine.getFilterExpressionsBuilder().build(qualifier));
        }
        WritePolicy writePolicy = writePolicyBuilder.build();

        if (binNames == null || binNames.length == 0) {
            return enrichPolicyWithTransaction(reactorClient, writePolicy)
                .flatMap(writePolicyEnriched ->
                    reactorClient.operate((WritePolicy) writePolicyEnriched, key, Operation.touch(), Operation.get()));
        }
        Operation[] operations = new Operation[binNames.length + 1];
        operations[0] = Operation.touch();

        for (int i = 1; i < operations.length; i++) {
            operations[i] = Operation.get(binNames[i - 1]);
        }
        return enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched -> reactorClient.operate((WritePolicy) writePolicyEnriched, key, operations));
    }

    private String[] getBinNamesFromTargetClass(Class<?> targetClass) {
        AerospikePersistentEntity<?> targetEntity = mappingContext.getRequiredPersistentEntity(targetClass);

        List<String> binNamesList = new ArrayList<>();

        targetEntity.doWithProperties(
            (PropertyHandler<AerospikePersistentProperty>) property -> {
                if (!property.isIdProperty()) {
                    binNamesList.add(property.getFieldName());
                }
            }
        );

        return binNamesList.toArray(new String[0]);
    }

    private Throwable translateError(Throwable e) {
        if (e instanceof AerospikeException ae) {
            return translateError(ae);
        }
        return e;
    }

    private Throwable translateCasThrowable(Throwable e, String operationName) {
        if (e instanceof AerospikeException ae) {
            return translateCasError(ae, "Failed to %s record due to versions mismatch".formatted(operationName));
        }
        return e;
    }

    private <T> Flux<T> findWithPostProcessing(String setName, Class<T> targetClass, Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Flux<T> results = findUsingQueryWithDistinctPredicate(setName, targetClass, getDistinctPredicate(query),
            query);
        results = applyPostProcessingOnResults(results, query);
        return results;
    }

    @SuppressWarnings("SameParameterValue")
    private <T> Flux<T> findWithPostProcessing(String setName, Class<T> targetClass, Sort sort, long offset,
                                               long limit) {
        verifyUnsortedWithOffset(sort, offset);
        Flux<T> results = find(setName, targetClass);
        results = applyPostProcessingOnResults(results, sort, offset, limit);
        return results;
    }

    @Override
    public <T, S> Flux<S> findUsingQueryWithoutPostProcessing(Class<T> entityClass, Class<S> targetClass, Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        return findUsingQueryWithDistinctPredicate(getSetName(entityClass), targetClass,
            getDistinctPredicate(query), query);
    }

    private void verifyUnsortedWithOffset(Sort sort, long offset) {
        if ((sort == null || sort.isUnsorted())
            && offset > 0) {
            throw new IllegalArgumentException("Unsorted query must not have offset value. " +
                "For retrieving paged results use sorted query.");
        }
    }

    private <T> Flux<T> applyPostProcessingOnResults(Flux<T> results, Query query) {
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

    private <T> Flux<T> applyPostProcessingOnResults(Flux<T> results, Sort sort, long offset, long limit) {
        if (sort != null && sort.isSorted()) {
            Comparator<T> comparator = getComparator(sort);
            results = results.sort(comparator);
        }

        if (offset > 0) {
            results = results.skip(offset);
        }

        if (limit > 0) {
            results = results.take(limit);
        }
        return results;
    }

    private <T> Flux<T> find(String setName, Class<T> targetClass) {
        return findRecordsUsingQuery(setName, targetClass, null)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
    }

    private <T> Flux<T> findUsingQueryWithDistinctPredicate(String setName, Class<T> targetClass,
                                                            Predicate<KeyRecord> distinctPredicate, Query query) {
        return findRecordsUsingQuery(setName, targetClass, query)
            .filter(distinctPredicate)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
    }

    private <T> Flux<KeyRecord> findRecordsUsingQuery(String setName, Class<T> targetClass, Query query) {
        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query with id
                return findByIdsWithoutMapping(getIdValue(idQualifier), setName, targetClass,
                    new Query(excludeIdQualifier(qualifier)));
            }
        }

        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass);
            return reactorQueryEngine.select(namespace, setName, binNames, query);
        }
        return reactorQueryEngine.select(namespace, setName, null, query);
    }

    private <T> Flux<KeyRecord> findByIdsWithoutMapping(Collection<?> ids, String setName,
                                                        Class<T> targetClass, Query query) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }

        BatchPolicy batchPolicy = getBatchPolicyFilterExp(query);

        return Flux.fromIterable(ids)
            .map(id -> getKey(id, setName))
            .flatMap(key -> getFromClient(batchPolicy, key, targetClass))
            .filter(keyRecord -> nonNull(keyRecord.record));
    }
}
