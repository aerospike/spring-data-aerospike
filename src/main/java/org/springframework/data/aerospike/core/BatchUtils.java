package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchResults;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.DELETE_OPERATION;
import static org.springframework.data.aerospike.core.TemplateUtils.operations;
import static org.springframework.data.aerospike.core.ValidationUtils.verifyUnsortedWithOffset;
import static org.springframework.data.aerospike.core.MappingUtils.getBinNamesFromTargetClass;
import static org.springframework.data.aerospike.core.ExceptionUtils.getOptimisticLockingFailureException;
import static org.springframework.data.aerospike.core.ValidationUtils.hasOptimisticLockingError;
import static org.springframework.data.aerospike.core.ValidationUtils.isEmpty;
import static org.springframework.data.aerospike.core.TemplateUtils.logEmptyItems;
import static org.springframework.data.aerospike.core.TemplateUtils.updateVersion;
import static org.springframework.data.aerospike.query.QualifierUtils.isQueryCriteriaNotNull;

/**
 * A utility class providing methods to facilitate processing of batch read and write operations.
 */
@Slf4j
public final class BatchUtils {

    private BatchUtils() {
        throw new UnsupportedOperationException("Utility class BatchUtils cannot be instantiated");
    }

    static <T> void applyBufferedBatchWrite(Iterable<T> documents, String setName, BaseAerospikeTemplate.OperationType operationType,
                                            TemplateContext templateContext) {
        int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchWriteSize();
        List<T> docsList = new ArrayList<>();

        for (T doc : documents) {
            if (batchWriteSizeMatch(batchSize, docsList.size())) {
                batchWriteAllDocuments(docsList, setName, operationType, templateContext);
                docsList.clear();
            }
            docsList.add(doc);
        }
        if (!docsList.isEmpty()) {
            batchWriteAllDocuments(docsList, setName, operationType, templateContext);
        }
    }

    private static <T> void batchWriteAllDocuments(List<T> documents,
                                                   String setName, BaseAerospikeTemplate.OperationType operationType,
                                                   TemplateContext templateContext) {
        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        switch (operationType) {
            case SAVE_OPERATION ->
                documents.forEach(document ->
                    batchWriteDataList.add(getBatchWriteForSave(document, setName, templateContext)));
            case INSERT_OPERATION ->
                documents.forEach(document ->
                    batchWriteDataList.add(getBatchWriteForInsert(document, setName, templateContext)));
            case UPDATE_OPERATION ->
                documents.forEach(document ->
                    batchWriteDataList.add(getBatchWriteForUpdate(document, setName, templateContext)));
            case DELETE_OPERATION ->
                documents.forEach(document ->
                    batchWriteDataList.add(getBatchWriteForDelete(document, setName, templateContext)));
            default -> throw new IllegalArgumentException("Unexpected operation name: " + operationType);
        }

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream().map(BatchWriteData::batchRecord).toList();
        try {
            BatchPolicy batchPolicy = (BatchPolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client,
                templateContext.client.copyBatchPolicyDefault());
            templateContext.client.operate(batchPolicy, batchWriteRecords);
        } catch (AerospikeException e) {
            // no exception is thrown for versions mismatch, only record's result code shows it
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }

        checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, operationType, templateContext);
    }

    private static <T> void checkForErrorsAndUpdateVersion(List<BatchWriteData<T>> batchWriteDataList,
                                                           List<BatchRecord> batchWriteRecords,
                                                           BaseAerospikeTemplate.OperationType operationType,
                                                           TemplateContext templateContext) {
        boolean errorsFound = false;
        String casErrorDocumentId = null;
        for (BatchWriteData<T> data : batchWriteDataList) {
            if (!errorsFound && batchRecordFailed(data.batchRecord(), false)) {
                errorsFound = true;
            }
            if (data.hasVersionProperty()) {
                if (!batchRecordFailed(data.batchRecord(), false)) {
                    if (operationType != DELETE_OPERATION) {
                        updateVersion(data.document(), data.batchRecord().record, templateContext);
                    }
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

    static void deleteByIds(Iterable<?> ids, String setName, boolean skipNonExisting,
                            TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }

        int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchWriteSize();
        List<Object> idsList = new ArrayList<>();
        for (Object id : ids) {
            if (batchWriteSizeMatch(batchSize, idsList.size())) {
                doDeleteByIds(idsList, setName, skipNonExisting, templateContext);
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            doDeleteByIds(idsList, setName, skipNonExisting, templateContext);
        }
    }

    static void doDeleteByIds(Collection<?> ids, String setName, boolean skipNonExisting,
                              TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }
        Key[] keys = getKeys(ids, setName, templateContext);

        // requires server ver. >= 6.0.0
        deleteAndHandleErrors(templateContext.client, keys, skipNonExisting, templateContext.exceptionTranslator);
    }

    static void deleteGroupedEntitiesByGroupedKeys(GroupedKeys groupedKeys, TemplateContext templateContext) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));
        deleteAndHandleErrors(templateContext.client, entitiesKeys.getKeys(), false,
            templateContext.exceptionTranslator);
    }

    static void deleteAndHandleErrors(IAerospikeClient client, Key[] keys, boolean skipNonExisting,
                                      AerospikeExceptionTranslator exceptionTranslator) {
        BatchResults results;
        try {
            BatchPolicy batchPolicy = (BatchPolicy) PolicyUtils.enrichPolicyWithTransaction(client,
                client.copyBatchPolicyDefault());
            results = client.delete(batchPolicy, null, keys);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, exceptionTranslator);
        }

        if (results.records == null) {
            throw new AerospikeException.BatchRecordArray(null,
                new AerospikeException("Errors during batch delete: resulting records are null"));
        }
        for (int i = 0; i < results.records.length; i++) {
            BatchRecord record = results.records[i];
            if (batchRecordFailed(record, skipNonExisting)) {
                throw new AerospikeException.BatchRecordArray(results.records,
                    new AerospikeException("Errors during batch delete"));
            }
        }
    }

    static Key[] getKeys(Collection<?> ids, String setName, TemplateContext templateContext) {
        return ids.stream()
            .map(id -> TemplateUtils.getKey(id, setName, templateContext))
            .toArray(Key[]::new);
    }

    static GroupedEntities findGroupedEntitiesByGroupedKeys(GroupedKeys groupedKeys, TemplateContext templateContext) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));
        BatchPolicy batchPolicy = (BatchPolicy) PolicyUtils.enrichPolicyWithTransaction(
            templateContext.client,
            templateContext.client.copyBatchPolicyDefault()
        );
        Record[] aeroRecords = templateContext.client.get(batchPolicy, entitiesKeys.getKeys());

        return MappingUtils.toGroupedEntities(entitiesKeys, aeroRecords, templateContext.converter);
    }

    static Record[] findByKeysUsingQuery(Key[] keys, @Nullable String[] binNames, String setName,
                                         @Nullable Query query, TemplateContext templateContext) {
        Assert.notNull(keys, "Keys must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        if (isQueryCriteriaNotNull(query)) {
            // Paginated queries with offset and no sorting (i.e. original order)
            // are only allowed for purely id queries, and not for other queries
            verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        }

        if (keys.length == 0) {
            return new Record[]{};
        }

        try {
            BatchPolicy batchPolicy = (BatchPolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client,
                PolicyUtils.getBatchPolicyFilterExp(query, templateContext));
            Record[] aeroRecords;
            if (binNames != null) {
                aeroRecords = templateContext.client.get(batchPolicy, keys, binNames);
            } else {
                aeroRecords = templateContext.client.get(batchPolicy, keys);
            }
            return aeroRecords;
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    /**
     * Returns stream of the records found
     */
    static Stream<?> findByIdsUsingQueryWithoutMapping(Collection<?> ids, String setName, Query query,
                                                       TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        Key[] keys = getKeys(ids, setName, templateContext);
        Record[] records = findByKeysUsingQuery(keys, null, setName, query, templateContext);
        return Arrays.stream(records);
    }

    // Without mapping results to entities
    static Stream<KeyRecord> findExistingByIdsWithoutMapping(Collection<?> ids, String setName, Class<?> targetClass,
                                                             Query query, TemplateContext templateContext) {
        Assert.notNull(ids, "Ids must not be null");
        if (ids.isEmpty()) {
            return Stream.empty();
        }

        Key[] keys = getKeys(ids, setName, templateContext);
        Record[] records =
            findByKeysUsingQuery(
                keys, 
                getBinNamesFromTargetClass(targetClass, templateContext.mappingContext), 
                setName, 
                query, 
                templateContext
            );
        return IntStream.range(0, records.length)
            .filter(index -> records[index] != null)
            .mapToObj(index -> new KeyRecord(keys[index], records[index]));
    }

    static <T> Flux<T> applyBufferedReactiveBatchWrite(Iterable<? extends T> documents, String setName,
                                                       BaseAerospikeTemplate.OperationType operationType, TemplateContext templateContext) {
        return Flux.defer(() -> {
            int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchWriteSize();

            // Create batches
            return createNullTolerantBatches(documents, batchSize)
                .concatMap(batch -> batchWriteAllDocumentsReactively(batch, setName, operationType, templateContext));
        });
    }

    static <T> Flux<T> batchWriteAllDocumentsReactively(List<T> documents, String setName, BaseAerospikeTemplate.OperationType operationType,
                                                        TemplateContext templateContext) {
        return Flux.defer(() -> {
            try {
                List<BatchWriteData<T>> batchWriteDataList = documents.stream().map(document ->
                    switch (operationType) {
                        case SAVE_OPERATION -> getBatchWriteForSave(document, setName, templateContext);
                        case INSERT_OPERATION -> getBatchWriteForInsert(document, setName, templateContext);
                        case UPDATE_OPERATION -> getBatchWriteForUpdate(document, setName, templateContext);
                        case DELETE_OPERATION -> getBatchWriteForDelete(document, setName, templateContext);
                    }
                ).toList();

                List<BatchRecord> batchWriteRecords = batchWriteDataList.stream()
                    .map(BatchWriteData::batchRecord)
                    .toList();

                IAerospikeReactorClient reactorClient = templateContext.reactorClient;
                return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                    .copyBatchPolicyDefault())
                    .flatMapMany(batchPolicyEnriched ->
                        batchWriteReactivelyAndCheckForErrors(
                            (BatchPolicy) batchPolicyEnriched, batchWriteRecords,
                            batchWriteDataList,
                            operationType,
                            templateContext
                        )
                    );
            } catch (Exception e) {
                return Flux.error(e);
            }
        });
    }

    static <T> Flux<T> batchWriteReactivelyAndCheckForErrors(BatchPolicy batchPolicy,
                                                             List<BatchRecord> batchWriteRecords,
                                                             List<BatchWriteData<T>> batchWriteDataList,
                                                             BaseAerospikeTemplate.OperationType operationType,
                                                             TemplateContext templateContext) {
        return templateContext.reactorClient
            .operate(batchPolicy, batchWriteRecords)
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator))
            .flatMap(ignore ->
                checkForErrorsAndUpdateVersionForReactive(batchWriteDataList, batchWriteRecords, operationType, templateContext))
            .flux()
            .flatMapIterable(list -> list.stream().map(BatchWriteData::document).toList());
    }

    static <T> Mono<List<BatchWriteData<T>>> checkForErrorsAndUpdateVersionForReactive(List<BatchWriteData<T>> batchWriteDataList,
                                                                                       List<BatchRecord> batchWriteRecords,
                                                                                       BaseAerospikeTemplate.OperationType operationType,
                                                                                       TemplateContext templateContext) {
        boolean errorsFound = false;
        String casErrorDocumentId = null;
        for (BatchWriteData<T> data : batchWriteDataList) {
            if (!errorsFound && batchRecordFailed(data.batchRecord(), false)) {
                errorsFound = true;
            }
            if (data.hasVersionProperty()) {
                if (!batchRecordFailed(data.batchRecord(), false)) {
                    if (operationType != DELETE_OPERATION) updateVersion(data.document(), data.batchRecord().record,
                        templateContext);
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

    static Mono<Void> deleteByIdsReactively(Iterable<?> ids, String setName, boolean skipNonExisting,
                                            TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return Mono.empty();
        }

        List<Object> idsList = new ArrayList<>();
        List<Mono<Void>> deleteResults = new ArrayList<>();
        for (Object id : ids) {
            if (batchWriteSizeMatch(templateContext.converter.getAerospikeDataSettings().getBatchWriteSize(),
                idsList.size()))
            {
                deleteResults.add(doDeleteByIdsReactively(new ArrayList<>(idsList), setName, skipNonExisting, templateContext));
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            deleteResults.add(doDeleteByIdsReactively(new ArrayList<>(idsList), setName, skipNonExisting, templateContext));
        }

        return Flux.concat(Flux.fromIterable(deleteResults)).then();
    }

    static Mono<Void> doDeleteByIdsReactively(Collection<?> ids, String setName, boolean skipNonExisting,
                                              TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return Mono.empty();
        }
        Key[] keys = ids.stream()
            .map(id -> TemplateUtils.getKey(id, setName, templateContext))
            .toArray(Key[]::new);

        return batchDeleteReactivelyAndCheckForErrors(templateContext.reactorClient, keys, skipNonExisting,
            templateContext.exceptionTranslator);
    }

    static Mono<Void> batchDeleteReactivelyAndCheckForErrors(IAerospikeReactorClient reactorClient, Key[] keys,
                                                             boolean skipNonExisting,
                                                             AerospikeExceptionTranslator exceptionTranslator) {
        Function<BatchResults, Mono<Void>> checkForErrors = results -> {
            if (results.records == null) {
                return Mono.error(new AerospikeException.BatchRecordArray(null,
                    new AerospikeException("Errors during batch delete: resulting records are null")));
            }
            for (BatchRecord record : results.records) {
                if (batchRecordFailed(record, skipNonExisting)) {
                    return Mono.error(new AerospikeException.BatchRecordArray(results.records,
                        new AerospikeException("Errors during batch delete")));
                }
            }
            return Mono.empty();
        };

        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.delete((BatchPolicy) batchPolicy, null, keys))
            .onErrorMap(e -> ExceptionUtils.translateError(e, exceptionTranslator))
            .flatMap(checkForErrors);
    }

    static Mono<Void> deleteEntitiesByGroupedKeysReactively(GroupedKeys groupedKeys, TemplateContext templateContext) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));

        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.delete((BatchPolicy) batchPolicy, null, entitiesKeys.getKeys()))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));

        return batchDeleteReactivelyAndCheckForErrors(reactorClient, entitiesKeys.getKeys(), false, templateContext.exceptionTranslator);
    }

    static Mono<GroupedEntities> findGroupedEntitiesByGroupedKeysReactively(BatchPolicy batchPolicy,
                                                                            GroupedKeys groupedKeys,
                                                                            TemplateContext templateContext) {
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));

        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, batchPolicy)
            .flatMap(batchPolicyEnriched -> reactorClient.get((BatchPolicy) batchPolicyEnriched,
                entitiesKeys.getKeys()))
            .map(item -> MappingUtils.toGroupedEntities(entitiesKeys, item.records, templateContext.converter))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    static BatchPolicy getBatchPolicyFilterExpForReactive(Query query, TemplateContext templateContext) {
        if (isQueryCriteriaNotNull(query)) {
            BatchPolicy batchPolicy = templateContext.reactorClient.getAerospikeClient().copyBatchPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            batchPolicy.filterExp = templateContext.reactorQueryEngine.getFilterExpressionsBuilder().build(qualifier);
            return batchPolicy;
        }
        return templateContext.reactorClient.getAerospikeClient().copyBatchPolicyDefault();
    }

    static Mono<KeyRecord> getFromClientReactively(BatchPolicy batchPolicy, Key key, Class<?> targetClass,
                                                   TemplateContext templateContext) {
        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        if (batchPolicy == null) batchPolicy = reactorClient.getAerospikeClient().copyBatchPolicyDefault();
        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass, templateContext.mappingContext);
            return PolicyUtils.enrichPolicyWithTransaction(reactorClient, batchPolicy)
                .flatMap(rPolicy -> reactorClient.get(rPolicy, key, binNames));
        } else {
            return PolicyUtils.enrichPolicyWithTransaction(reactorClient, batchPolicy)
                .flatMap(rPolicy -> reactorClient.get(rPolicy, key));
        }
    }

    static <T> BatchWriteData<T> getBatchWriteForSave(T document, String setName,
                                                             TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);

        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy = PolicyUtils.expectGenerationCasAwareBatchPolicy(data, templateContext.batchWritePolicyDefault);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = TemplateUtils.getPutAndGetHeaderOperations(data, true);
        } else {
            policy =
                PolicyUtils.ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE, templateContext.batchWritePolicyDefault);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    static <T> BatchWriteData<T> getBatchWriteForInsert(T document, String setName,
                                                               TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);

        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        BatchWritePolicy policy =
            PolicyUtils.ignoreGenerationBatchPolicy(data, RecordExistsAction.CREATE_ONLY, templateContext.batchWritePolicyDefault);
        Operation[] operations;
        if (entity.hasVersionProperty()) {
            operations = TemplateUtils.getPutAndGetHeaderOperations(data, false);
        } else {
            operations = operations(data.getBinsAsArray(), Operation::put);
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    static <T> BatchWriteData<T> getBatchWriteForUpdate(T document, String setName,
                                                               TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);

        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy =
                PolicyUtils.expectGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY, templateContext.batchWritePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = TemplateUtils.getPutAndGetHeaderOperations(data, true);
        } else {
            policy = PolicyUtils.ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY, templateContext.batchWritePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = Stream.concat(Stream.of(Operation.delete()), data.getBins().stream()
                .map(Operation::put)).toArray(Operation[]::new);
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    static <T> BatchWriteData<T> getBatchWriteForDelete(T document, String setName,
                                                               TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);

        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy = PolicyUtils.expectGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.batchWritePolicyDefault);
        } else {
            policy = PolicyUtils.ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.batchWritePolicyDefault);
        }
        Operation[] operations = Operation.array(Operation.delete());

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    static boolean batchWriteSizeMatch(int batchSize, int currentSize) {
        return batchSize > 0 && currentSize == batchSize;
    }

    static boolean batchRecordFailed(BatchRecord batchRecord, boolean skipNonExisting) {
        int resultCode = batchRecord.resultCode;
        if (skipNonExisting) {
            return resultCode != ResultCode.OK && resultCode != ResultCode.KEY_NOT_FOUND_ERROR;
        }
        return resultCode != ResultCode.OK || batchRecord.record == null;
    }

    /**
     * Creates batches from an iterable source, tolerating null values. Each batch will have at most batchSize elements
     *
     * @param source    The source iterable containing elements to batch
     * @param batchSize The maximum size of each batch
     * @return A Flux emitting lists of batched elements
     */
    static <T> Flux<List<T>> createNullTolerantBatches(Iterable<? extends T> source, int batchSize) {
        return Flux.create(sink -> {
            try {
                List<T> currentBatch = new ArrayList<>();

                for (T item : source) {
                    // Add item to the current batch (even if null)
                    currentBatch.add(item);

                    // When we hit batch size, emit the batch and start a new one
                    if (batchWriteSizeMatch(batchSize, currentBatch.size())) {
                        sink.next(new ArrayList<>(currentBatch));
                        currentBatch.clear();
                    }
                }

                // Emit any remaining items in the final batch
                if (!currentBatch.isEmpty()) {
                    sink.next(new ArrayList<>(currentBatch));
                }

                sink.complete();
            } catch (Exception e) {
                sink.error(e);
            }
        });
    }
}
