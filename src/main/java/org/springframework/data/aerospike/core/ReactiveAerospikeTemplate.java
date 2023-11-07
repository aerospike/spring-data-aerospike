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
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
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

import static com.aerospike.client.ResultCode.KEY_NOT_FOUND_ERROR;
import static java.util.Objects.nonNull;
import static org.springframework.data.aerospike.core.CoreUtils.getDistinctPredicate;
import static org.springframework.data.aerospike.core.CoreUtils.operations;
import static org.springframework.data.aerospike.core.TemplateUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.core.TemplateUtils.getIdValue;
import static org.springframework.data.aerospike.core.TemplateUtils.queryCriteriaIsNotNull;
import static org.springframework.data.aerospike.query.QualifierUtils.getOneIdQualifier;
import static org.springframework.data.aerospike.query.QualifierUtils.validateQualifiers;
import static org.springframework.data.aerospike.utility.Utils.allArrayElementsAreNull;

/**
 * Primary implementation of {@link ReactiveAerospikeOperations}.
 *
 * @author Igor Ermolenko
 * @author Volodymyr Shpynta
 * @author Yevhen Tsyba
 */
@Slf4j
public class ReactiveAerospikeTemplate extends BaseAerospikeTemplate implements ReactiveAerospikeOperations,
    IndexesCacheRefresher {

    private static final Pattern INDEX_EXISTS_REGEX_PATTERN = Pattern.compile("^FAIL:(-?\\d+).*$");
    private final IAerospikeReactorClient reactorClient;
    private final ReactorQueryEngine reactorQueryEngine;
    private final ReactorIndexRefresher reactorIndexRefresher;

    public ReactiveAerospikeTemplate(IAerospikeReactorClient reactorClient,
                                     String namespace,
                                     MappingAerospikeConverter converter,
                                     AerospikeMappingContext mappingContext,
                                     AerospikeExceptionTranslator exceptionTranslator,
                                     ReactorQueryEngine queryEngine, ReactorIndexRefresher reactorIndexRefresher) {
        super(namespace, converter, mappingContext, exceptionTranslator, reactorClient.getWritePolicyDefault());
        Assert.notNull(reactorClient, "Aerospike reactor client must not be null!");
        this.reactorClient = reactorClient;
        this.reactorQueryEngine = queryEngine;
        this.reactorIndexRefresher = reactorIndexRefresher;
    }

    @Override
    public void refreshIndexesCache() {
        reactorIndexRefresher.refreshIndexes();
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
            WritePolicy policy = expectGenerationCasAwareSavePolicy(data);
            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));

            return doPersistWithVersionAndHandleCasError(document, data, policy, operations);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.UPDATE);
            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));

            return doPersistAndHandleError(document, data, policy, operations);
        }
    }

    @Override
    public <T> Flux<T> saveAll(Iterable<T> documents) {
        Assert.notNull(documents, "Documents for saving must not be null!");
        return saveAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> saveAll(Iterable<T> documents, String setName) {
        Assert.notNull(documents, "Documents for saving must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        documents.forEach(document -> batchWriteDataList.add(getBatchWriteForSave(document, setName)));

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();

        return batchWriteAndCheckForErrors(batchWriteRecords, batchWriteDataList, "save");
    }

    private <T> Flux<T> batchWriteAndCheckForErrors(List<BatchRecord> batchWriteRecords,
                                                    List<BatchWriteData<T>> batchWriteDataList, String commandName) {
        // requires server ver. >= 6.0.0
        return reactorClient.operate(null, batchWriteRecords)
            .onErrorMap(this::translateError)
            .flatMap(ignore -> checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, commandName))
            .flux()
            .flatMapIterable(list -> list.stream().map(BatchWriteData::document).toList());
    }

    private <T> Mono<List<BatchWriteData<T>>> checkForErrorsAndUpdateVersion(List<BatchWriteData<T>> batchWriteDataList,
                                                                             List<BatchRecord> batchWriteRecords,
                                                                             String commandName) {
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
        WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.CREATE_ONLY);

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
            return doPersistWithVersionAndHandleError(document, data, policy, operations);
        } else {
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            return doPersistAndHandleError(document, data, policy, operations);
        }
    }

    @Override
    public <T> Flux<T> insertAll(Iterable<? extends T> documents) {
        return insertAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> insertAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(documents, "Documents for insert must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        documents.forEach(document -> batchWriteDataList.add(getBatchWriteForInsert(document, setName)));

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();

        return batchWriteAndCheckForErrors(batchWriteRecords, batchWriteDataList, "insert");
    }

    @Override
    public <T> Mono<T> persist(T document, WritePolicy policy) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(policy, "Policy must not be null!");
        return persist(document, policy, getSetName(document));
    }

    @Override
    public <T> Mono<T> persist(T document, WritePolicy policy, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(policy, "Policy must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
        return doPersistAndHandleError(document, data, policy, operations);
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
            WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()), Operation.array(Operation.getHeader()));
            return doPersistWithVersionAndHandleCasError(document, data, policy, operations);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
            return doPersistAndHandleError(document, data, policy, operations);
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
            WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put, null,
                Operation.array(Operation.getHeader()));
            return doPersistWithVersionAndHandleCasError(document, data, policy, operations);
        } else {
            WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.UPDATE_ONLY);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            return doPersistAndHandleError(document, data, policy, operations);
        }
    }

    @Override
    public <T> Flux<T> updateAll(Iterable<? extends T> documents) {
        return updateAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> updateAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(documents, "Documents for saving must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        documents.forEach(document -> batchWriteDataList.add(getBatchWriteForUpdate(document, setName)));

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();

        return batchWriteAndCheckForErrors(batchWriteRecords, batchWriteDataList, "update");
    }

    @Deprecated
    @Override
    public <T> Mono<Void> delete(Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");

        try {
            String set = getSetName(entityClass);
            return Mono.fromRunnable(
                () -> reactorClient.getAerospikeClient().truncate(null, this.namespace, set, null));
        } catch (AerospikeException e) {
            throw translateError(e);
        }
    }

    @Deprecated
    @Override
    public <T> Mono<Boolean> delete(Object id, Class<T> entityClass) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

        return reactorClient
            .delete(ignoreGenerationDeletePolicy(), getKey(id, entity))
            .map(k -> true)
            .onErrorMap(this::translateError);
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

        return reactorClient
            .delete(ignoreGenerationDeletePolicy(), data.getKey())
            .map(key -> true)
            .onErrorMap(this::translateError);
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

        return reactorClient
            .delete(ignoreGenerationDeletePolicy(), getKey(id, setName))
            .map(k -> true)
            .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Void> deleteByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");

        return deleteByIds(ids, getSetName(entityClass));
    }

    @Override
    public Mono<Void> deleteByIds(Iterable<?> ids, String setName) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        return deleteByIds(IterableConverter.toList(ids), setName);
    }

    @Override
    public <T> Mono<Void> deleteByIds(Collection<?> ids, Class<T> entityClass) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");

        return deleteByIds(ids, getSetName(entityClass));
    }

    @Override
    public Mono<Void> deleteByIds(Collection<?> ids, String setName) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        Key[] keys = ids.stream()
            .map(id -> getKey(id, setName))
            .toArray(Key[]::new);

        return batchDeleteAndCheckForErrors(reactorClient, keys);
    }

    private Mono<Void> batchDeleteAndCheckForErrors(IAerospikeReactorClient reactorClient, Key[] keys) {
        Function<BatchResults, Mono<Void>> checkForErrors = results -> {
            for (BatchRecord record : results.records) {
                if (batchRecordFailed(record)) {
                    return Mono.error(new AerospikeException.BatchRecordArray(results.records,
                        new AerospikeException("Errors during batch delete")));
                }
            }
            return Mono.empty();
        };

        // requires server ver. >= 6.0.0
        return reactorClient.delete(null, null, keys)
            .onErrorMap(this::translateError)
            .flatMap(checkForErrors);
    }

    @Override
    public Mono<Void> deleteByIds(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");
        Assert.notNull(groupedKeys.getEntitiesKeys(), "Entities keys must not be null!");
        Assert.notEmpty(groupedKeys.getEntitiesKeys(), "Entities keys must not be empty!");

        return deleteEntitiesByGroupedKeys(groupedKeys);
    }

    private Mono<Void> deleteEntitiesByGroupedKeys(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));

        reactorClient.delete(null, null, entitiesKeys.getKeys())
            .doOnError(this::translateError);

        return batchDeleteAndCheckForErrors(reactorClient, entitiesKeys.getKeys());
    }

    @Override
    public <T> Mono<Void> deleteAll(Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");

        return deleteAll(getSetName(entityClass));
    }

    @Override
    public Mono<Void> deleteAll(String setName) {
        Assert.notNull(setName, "Set name must not be null!");

        try {
            return Mono.fromRunnable(
                () -> reactorClient.getAerospikeClient().truncate(null, this.namespace, setName, null));
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

        WritePolicy writePolicy = WritePolicyBuilder.builder(this.writePolicyDefault)
            .expiration(data.getExpiration())
            .build();

        return executeOperationsOnValue(document, data, operations, writePolicy);
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

        WritePolicy writePolicy = WritePolicyBuilder.builder(this.writePolicyDefault)
            .expiration(data.getExpiration())
            .build();

        Operation[] operations = {Operation.add(new Bin(binName, value)), Operation.get(binName)};
        return executeOperationsOnValue(document, data, operations, writePolicy);
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
        return executeOperationsOnValue(document, data, operations, null);
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
        return executeOperationsOnValue(document, data, operations, null);
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
        return executeOperationsOnValue(document, data, operations, null);
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
        return executeOperationsOnValue(document, data, operations, null);
    }

    private <T> Mono<T> executeOperationsOnValue(T document, AerospikeWriteData data, Operation[] operations,
                                                 WritePolicy writePolicy) {
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
            return getAndTouch(key, entity.getExpiration(), null)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
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
            return getAndTouch(key, entity.getExpiration(), binNames)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, targetClass, keyRecord.record))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(this::translateError);
        } else {
            return reactorClient.get(null, key, binNames)
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

        return Flux.fromIterable(ids)
            .map(id -> getKey(id, setName))
            .flatMap(reactorClient::get)
            .filter(keyRecord -> nonNull(keyRecord.record))
            .map(keyRecord -> mapToEntity(keyRecord.key, targetClass, keyRecord.record));
    }

    @Override
    public Mono<GroupedEntities> findByIds(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");

        if (groupedKeys.getEntitiesKeys() == null || groupedKeys.getEntitiesKeys().isEmpty()) {
            return Mono.just(GroupedEntities.builder().build());
        }

        return findGroupedEntitiesByGroupedKeys(groupedKeys);
    }

    private Mono<GroupedEntities> findGroupedEntitiesByGroupedKeys(GroupedKeys groupedKeys) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));

        return reactorClient.get(null, entitiesKeys.getKeys())
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

        Qualifier[] qualifiers = queryCriteriaIsNotNull(query) ?
            new Qualifier[]{query.getCriteria().getCriteriaObject()} : null;
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(),
                "Touch on read is not supported for entity without expiration property");
            return getAndTouch(key, entity.getExpiration(), binNames, qualifiers)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(this::translateError);
        } else {
            Policy policy = null;
            if (qualifiers != null && qualifiers.length > 0) {
                policy = new Policy(reactorClient.getReadPolicyDefault());
                policy.filterExp = reactorQueryEngine.getFilterExpressionsBuilder().build(qualifiers);
            }
            return reactorClient.get(policy, key, binNames)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record))
                .onErrorMap(this::translateError);
        }
    }

    @Override
    public <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                              Query query) {
        return findByIdsUsingQuery(ids, entityClass, targetClass, getSetName(entityClass), query);
    }

    @Override
    public <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                              String setName, Query query) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }

        Qualifier[] qualifiers = queryCriteriaIsNotNull(query) && query.getCriteria() != null ?
            new Qualifier[]{query.getCriteria().getCriteriaObject()} : null;
        BatchPolicy policy = getBatchPolicyFilterExp(qualifiers);

        Class<?> target;
        if (targetClass != null && targetClass != entityClass) {
            target = targetClass;
        } else {
            target = entityClass;
        }

        return Flux.fromIterable(ids)
            .map(id -> getKey(id, setName))
            .flatMap(key -> getFromClient(policy, key, targetClass))
            .filter(keyRecord -> nonNull(keyRecord.record))
            .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record));
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

        return findUsingQueryWithPostProcessing(setName, targetClass, query);
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> entityClass, Filter filter) {
        return find(query, entityClass, getSetName(entityClass), filter);
    }

    @Override
    public <T, S> Flux<?> find(Query query, Class<T> entityClass, Class<S> targetClass, Filter filter) {
        return findRecordsUsingQualifiers(getSetName(entityClass), targetClass, filter)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
    }

    @Override
    public <T> Flux<T> find(Query query, Class<T> targetClass, String setName, Filter filter) {
        Qualifier criteria = queryCriteriaIsNotNull(query) ? query.getCriteria().getCriteriaObject() : null;
        return findRecordsUsingQualifiers(setName, targetClass, filter, criteria)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
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

        return findUsingQualifiers(setName, targetClass, null, (Qualifier[]) null);
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

        return findUsingQualifiersWithPostProcessing(setName, targetClass, sort, offset, limit,
            null, (Qualifier[]) null);
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

        return findUsingQualifiersWithPostProcessing(setName, targetClass, sort, offset, limit,
            null, (Qualifier[]) null);
    }

    private BatchPolicy getBatchPolicyFilterExp(Qualifier[] qualifiers) {
        if (qualifiers != null && qualifiers.length > 0) {
            BatchPolicy policy = new BatchPolicy(reactorClient.getAerospikeClient().getBatchPolicyDefault());
            policy.filterExp = reactorQueryEngine.getFilterExpressionsBuilder().build(qualifiers);
            return policy;
        }
        return null;
    }

    private Mono<KeyRecord> getFromClient(BatchPolicy finalPolicy, Key key, Class<?> targetClass) {
        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass);
            return reactorClient.get(finalPolicy, key, binNames);
        } else {
            return reactorClient.get(finalPolicy, key);
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
        return reactorClient.exists(key)
            .map(Objects::nonNull)
            .defaultIfEmpty(false)
            .onErrorMap(this::translateError);
    }

    @Override
    public <T> Mono<Boolean> existsByQuery(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Class must not be null!");
        return existsByQuery(query, entityClass, getSetName(entityClass));
    }

    @Override
    public <T> Mono<Boolean> existsByQuery(Query query, Class<T> entityClass, String setName) {
        Assert.notNull(query, "Query passed in to exist can't be null");
        Assert.notNull(entityClass, "Class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        return find(query, entityClass, setName).hasElements();
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

    private long countSet(String setName) {
        Node[] nodes = reactorClient.getAerospikeClient().getNodes();

        int replicationFactor = Utils.getReplicationFactor(nodes, this.namespace);

        long totalObjects = Arrays.stream(nodes)
            .mapToLong(node -> Utils.getObjectsCount(node, this.namespace, setName))
            .sum();

        return (nodes.length > 1) ? (totalObjects / replicationFactor) : totalObjects;
    }

    @Override
    public <T> Mono<Long> count(Query query, Class<T> entityClass) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(entityClass, "Class must not be null!");

        return count(query, getSetName(entityClass));
    }

    @Override
    public Mono<Long> count(Query query, String setName) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(setName, "Set for count must not be null!");

        return findRecordsUsingQuery(setName, query).count();
    }

    private Flux<KeyRecord> findRecordsUsingQuery(String setName, Query query) {
        Assert.notNull(query, "Query must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        return findRecordsUsingQualifiers(setName, null, null, qualifier);
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

        return reactorClient.createIndex(null, this.namespace,
                setName, indexName, binName, indexType, indexCollectionType, ctx)
            .then(reactorIndexRefresher.refreshIndexes())
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

        return reactorClient.dropIndex(null, this.namespace, setName, indexName)
            .then(reactorIndexRefresher.refreshIndexes())
            .onErrorMap(this::translateError);
    }

    @Override
    public Mono<Boolean> indexExists(String indexName) {
        Assert.notNull(indexName, "Index name must not be null!");

        try {
            Node[] nodes = reactorClient.getAerospikeClient().getNodes();
            for (Node node : nodes) {
                String response = Info.request(node, "sindex-exists:ns=" + namespace + ";indexname=" + indexName);
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

    private <T> Mono<T> doPersistAndHandleError(T document, AerospikeWriteData data, WritePolicy policy,
                                                Operation[] operations) {
        return reactorClient
            .operate(policy, data.getKey(), operations)
            .map(docKey -> document)
            .onErrorMap(this::translateError);
    }

    private <T> Mono<T> doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data, WritePolicy policy,
                                                              Operation[] operations) {
        return putAndGetHeader(data, policy, operations)
            .map(newRecord -> updateVersion(document, newRecord))
            .onErrorMap(AerospikeException.class, this::translateCasError);
    }

    private <T> Mono<T> doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy policy,
                                                           Operation[] operations) {
        return putAndGetHeader(data, policy, operations)
            .map(newRecord -> updateVersion(document, newRecord))
            .onErrorMap(AerospikeException.class, this::translateError);
    }

    private Mono<Record> putAndGetHeader(AerospikeWriteData data, WritePolicy policy, Operation[] operations) {
        return reactorClient.operate(policy, data.getKey(), operations)
            .map(keyRecord -> keyRecord.record);
    }

    private Mono<KeyRecord> getAndTouch(Key key, int expiration, String[] binNames, Qualifier... qualifiers) {
        WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(this.writePolicyDefault)
            .expiration(expiration);

        if (qualifiers != null && qualifiers.length > 0) {
            writePolicyBuilder.filterExp(reactorQueryEngine.getFilterExpressionsBuilder().build(qualifiers));
        }
        WritePolicy writePolicy = writePolicyBuilder.build();

        if (binNames == null || binNames.length == 0) {
            return reactorClient.operate(writePolicy, key, Operation.touch(), Operation.get());
        }
        Operation[] operations = new Operation[binNames.length + 1];
        operations[0] = Operation.touch();

        for (int i = 1; i < operations.length; i++) {
            operations[i] = Operation.get(binNames[i - 1]);
        }
        return reactorClient.operate(writePolicy, key, operations);
    }

    private String[] getBinNamesFromTargetClass(Class<?> targetClass) {
        AerospikePersistentEntity<?> targetEntity = mappingContext.getRequiredPersistentEntity(targetClass);

        List<String> binNamesList = new ArrayList<>();

        targetEntity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) property
            -> binNamesList.add(property.getFieldName()));

        return binNamesList.toArray(new String[0]);
    }

    private Throwable translateError(Throwable e) {
        if (e instanceof AerospikeException ae) {
            return translateError(ae);
        }
        return e;
    }

    private <T> Flux<T> findUsingQueryWithPostProcessing(String setName, Class<T> targetClass, Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Qualifier qualifier = query.getCriteria().getCriteriaObject();
        Flux<T> results = findUsingQualifiersWithDistinctPredicate(setName, targetClass,
            getDistinctPredicate(query), qualifier);
        results = applyPostProcessingOnResults(results, query);
        return results;
    }

    @SuppressWarnings("SameParameterValue")
    private <T> Flux<T> findUsingQualifiersWithPostProcessing(String setName, Class<T> targetClass, Sort sort,
                                                              long offset, long limit, Filter filter,
                                                              Qualifier... qualifiers) {
        verifyUnsortedWithOffset(sort, offset);
        Flux<T> results = findUsingQualifiers(setName, targetClass, filter, qualifiers);
        results = applyPostProcessingOnResults(results, sort, offset, limit);
        return results;
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

    private <T> Flux<T> findUsingQualifiers(String setName, Class<T> targetClass, Filter filter,
                                            Qualifier... qualifiers) {
        return findRecordsUsingQualifiers(setName, targetClass, filter, qualifiers)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
    }

    private <T> Flux<T> findUsingQualifiersWithDistinctPredicate(String setName, Class<T> targetClass,
                                                                 Predicate<KeyRecord> distinctPredicate,
                                                                 Qualifier... qualifiers) {
        return findRecordsUsingQualifiers(setName, targetClass, null, qualifiers)
            .filter(distinctPredicate)
            .map(keyRecord -> mapToEntity(keyRecord, targetClass));
    }

    private <T> Flux<KeyRecord> findRecordsUsingQualifiers(String setName, Class<T> targetClass, Filter filter,
                                                           Qualifier... qualifiers) {
        if (qualifiers != null && qualifiers.length > 0 && !allArrayElementsAreNull(qualifiers)) {
            validateQualifiers(qualifiers);

            Qualifier idQualifier = getOneIdQualifier(qualifiers);
            if (idQualifier != null) {
                // a special flow if there is id given
                return findByIdsWithoutMapping(getIdValue(idQualifier), setName, targetClass,
                    excludeIdQualifier(qualifiers));
            }
        }

        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass);
            return this.reactorQueryEngine.select(this.namespace, setName, binNames, filter, qualifiers);
        } else {
            return this.reactorQueryEngine.select(this.namespace, setName, filter, qualifiers);
        }
    }

    private <T> Flux<KeyRecord> findByIdsWithoutMapping(Collection<?> ids, String setName,
                                                        Class<T> targetClass,
                                                        Qualifier... qualifiers) {
        Assert.notNull(ids, "List of ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }

        BatchPolicy policy = getBatchPolicyFilterExp(qualifiers);

        return Flux.fromIterable(ids)
            .map(id -> getKey(id, setName))
            .flatMap(key -> getFromClient(policy, key, targetClass))
            .filter(keyRecord -> nonNull(keyRecord.record));
    }
}
