package org.springframework.data.aerospike.core;

import com.aerospike.client.*;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.aerospike.client.reactor.dto.KeysRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.OptimisticLockingFailureException;
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
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.DELETE_OPERATION;
import static org.springframework.data.aerospike.core.MappingUtils.getBinNamesFromTargetClassOrNull;
import static org.springframework.data.aerospike.core.MappingUtils.getKeys;
import static org.springframework.data.aerospike.core.MappingUtils.getTargetClass;
import static org.springframework.data.aerospike.core.MappingUtils.mapToEntity;
import static org.springframework.data.aerospike.core.PolicyUtils.*;
import static org.springframework.data.aerospike.core.TemplateUtils.*;
import static org.springframework.data.aerospike.core.ValidationUtils.verifyUnsortedWithOffset;
import static org.springframework.data.aerospike.core.ValidationUtils.hasOptimisticLockingError;
import static org.springframework.data.aerospike.core.ValidationUtils.isEmpty;
import static org.springframework.data.aerospike.query.QualifierUtils.isQueryCriteriaNotNull;
import static org.springframework.data.aerospike.util.Utils.iterableToList;

/**
 * A utility class providing methods to facilitate processing of batch read and write operations.
 */
@Slf4j
public final class BatchUtils {

    private BatchUtils() {
        throw new UnsupportedOperationException("Utility class BatchUtils cannot be instantiated");
    }

    /**
     * Applies a chunked batch write operation to a collection of documents. This method chunks the input documents into
     * batches based on the configured batch write size and performs the specified operation (save, insert, update,
     * delete) for each batch. It handles the iteration and batching, delegating the actual write operation to
     * {@link #batchWriteAllDocuments(Collection, String, BaseAerospikeTemplate.OperationType, BatchPolicy,
     * TemplateContext)}.
     *
     * @param <T>             The type of documents
     * @param documents       Iterable of documents to be written
     * @param setName         The name of the set to store the documents
     * @param operationType   The type of write operation to perform
     * @param templateContext {@link TemplateContext} containing Aerospike client, converter, and other necessary
     *                        components
     */
    static <T> void applyBatchWriteInChunks(Iterable<T> documents, String setName,
                                            BaseAerospikeTemplate.OperationType operationType,
                                            TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        BatchPolicy batchPolicy = (BatchPolicy) enrichPolicyWithTransaction(templateContext.client,
            templateContext.client.copyBatchPolicyDefault());

        int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchWriteSize();
        if (batchSize <= 0) {
            // For non-positive batchSize, write records straight away without chunking
            batchWriteAllDocuments(iterableToList(documents), setName, operationType, batchPolicy, templateContext);
            return;
        }

        List<T> docsList = new ArrayList<>();
        for (T doc : documents) {
            if (batchSizeMatch(batchSize, docsList.size())) {
                batchWriteAllDocuments(docsList, setName, operationType, batchPolicy, templateContext);
                docsList.clear();
            }
            docsList.add(doc);
        }
        if (!docsList.isEmpty()) {
            batchWriteAllDocuments(docsList, setName, operationType, batchPolicy, templateContext);
        }
    }

    /**
     * Performs a batch write operation for a given collection of documents. This method also receives {@param setName},
     * {@link BaseAerospikeTemplate.OperationType}, {@link BatchPolicy} and {@link TemplateContext}, and executes the
     * batch operation using the Aerospike client. It also includes error checking and version updating after the batch
     * write completes.
     *
     * @param <T>             The type of the documents
     * @param documents       Collection of documents to be written in this batch
     * @param setName         The name of the set to store the documents
     * @param operationType   The type of write operation to perform
     * @param batchPolicy     {@link BatchPolicy} to use
     * @param templateContext {@link TemplateContext} containing Aerospike client, converter, and other necessary
     *                        components
     * @throws IllegalArgumentException            if an unexpected operation type is provided
     * @throws AerospikeException                  if an Aerospike-specific error occurs during the batch operation
     * @throws AerospikeException.BatchRecordArray if results of batch write contain either an error or a null record
     * @throws OptimisticLockingFailureException   if there is version mismatch (CAS error)
     */
    private static <T> void batchWriteAllDocuments(Collection<T> documents,
                                                   String setName, BaseAerospikeTemplate.OperationType operationType,
                                                   BatchPolicy batchPolicy, TemplateContext templateContext) {
        List<BatchWriteData<T>> batchWriteDataList = new ArrayList<>();
        switch (operationType) {
            case SAVE_OPERATION -> documents.forEach(document ->
                batchWriteDataList.add(getBatchWriteForSave(document, setName, templateContext)));
            case INSERT_OPERATION -> documents.forEach(document ->
                batchWriteDataList.add(getBatchWriteForInsert(document, setName, templateContext)));
            case UPDATE_OPERATION -> documents.forEach(document ->
                batchWriteDataList.add(getBatchWriteForUpdate(document, setName, templateContext)));
            case DELETE_OPERATION -> documents.forEach(document ->
                batchWriteDataList.add(getBatchWriteForDelete(document, setName, templateContext)));
            default -> throw new IllegalArgumentException("Unexpected operation name: " + operationType);
        }

        List<BatchRecord> batchWriteRecords = batchWriteDataList.stream()
            .map(BatchWriteData::batchRecord)
            .collect(Collectors.toList());
        try {
            templateContext.client.operate(batchPolicy, batchWriteRecords);
        } catch (AerospikeException e) {
            // no exception is thrown for versions mismatch, only record's result code shows it
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }

        checkForErrorsAndUpdateVersion(batchWriteDataList, batchWriteRecords, operationType, templateContext);
    }

    /**
     * Checks for errors in the results of a batch write operation and updates document versions if applicable. This
     * method iterates through the resulting {@link BatchRecord}s to identify the following: result code of a batch
     * record not equal to {@link ResultCode#OK}, null batch record and optimistic locking error (CAS mismatch). If
     * errors are found, they are translated into corresponding exceptions.
     *
     * @param <T>                The type of the documents
     * @param batchWriteDataList A collection of {@link BatchWriteData} objects, containing the original documents and
     *                           their corresponding batch records
     * @param batchWriteRecords  A list of {@link BatchRecord} objects returned by the Aerospike client as a result of
     *                           the batch operation
     * @param operationType      The type of write operation that was performed
     * @param templateContext    {@link TemplateContext} containing Aerospike client, converter, and other necessary
     *                           components
     * @throws com.aerospike.client.AerospikeException.BatchRecordArray  if errors were found in results of the batch
     *                                                                   write
     * @throws org.springframework.dao.OptimisticLockingFailureException if an optimistic locking error (CAS mismatch)
     *                                                                   is detected
     */
    private static <T> void checkForErrorsAndUpdateVersion(Collection<BatchWriteData<T>> batchWriteDataList,
                                                           Collection<BatchRecord> batchWriteRecords,
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
                throw new OptimisticLockingFailureException(
                    "Failed to %s the record with ID '%s' due to versions mismatch"
                        .formatted(operationType, casErrorDocumentId), null);
            }
            AerospikeException e = new AerospikeException("Errors during batch " + operationType);
            throw new AerospikeException.BatchRecordArray(batchWriteRecords.toArray(BatchRecord[]::new), e);
        }
    }

    /**
     * Deletes documents by their IDs in chunked batches. This method handles the batching logic and delegates to
     * {@link #doDeleteByIds(Collection, String, boolean, TemplateContext)} for the actual deletion.
     *
     * @param ids             Iterable of document IDs to delete
     * @param setName         The name of the set to store the documents
     * @param skipNonExisting A boolean indicating whether to skip error check for non-existing resulting records
     *                        (allowing {@link ResultCode#OK} and {@link ResultCode#KEY_NOT_FOUND_ERROR}, regardless of
     *                        the record being null) {@link TemplateContext} containing Aerospike client, converter, and
     *                        other necessary components
     * @throws IllegalArgumentException if given set name is null
     * @throws AerospikeException       if there is an error during batch delete
     */
    static void deleteByIds(Iterable<?> ids, String setName, boolean skipNonExisting,
                            TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }

        int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchWriteSize();
        if (batchSize <= 0) {
            // For non-positive batchSize, delete records straight away without chunking
            doDeleteByIds(iterableToList(ids), setName, skipNonExisting, templateContext);
            return;
        }

        List<Object> idsList = new ArrayList<>();
        for (Object id : ids) {
            if (batchSizeMatch(batchSize, idsList.size())) {
                doDeleteByIds(idsList, setName, skipNonExisting, templateContext);
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            doDeleteByIds(idsList, setName, skipNonExisting, templateContext);
        }
    }

    /**
     * Performs batch deletion of documents by their IDs. This method converts the provided IDs into Aerospike
     * {@link Key} objects and then calls
     * {@link #deleteAndHandleErrors(IAerospikeClient, Key[], boolean, AerospikeExceptionTranslator)} to execute the
     * deletion and handle any errors.
     *
     * @param ids             A collection of document IDs to delete
     * @param setName         The name of the set to store the documents
     * @param skipNonExisting A boolean indicating whether to skip error check for non-existing resulting records
     *                        (allowing {@link ResultCode#OK} and {@link ResultCode#KEY_NOT_FOUND_ERROR}, regardless of
     *                        the record being null) {@link TemplateContext} containing Aerospike client, converter, and
     *                        other necessary components
     * @throws IllegalArgumentException if the set name is null
     * @throws AerospikeException       if there is an error during batch delete
     */
    private static void doDeleteByIds(Collection<?> ids, String setName, boolean skipNonExisting,
                                      TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }
        Key[] keys = MappingUtils.getKeys(ids, setName, templateContext).toArray(Key[]::new);

        // requires server ver. >= 6.0.0
        deleteAndHandleErrors(templateContext.client, keys, skipNonExisting, templateContext.exceptionTranslator);
    }

    /**
     * Deletes entities based on a collection of grouped keys. This method first converts the {@link GroupedKeys} into
     * {@link EntitiesKeys} and then performs a batch delete operation using the Aerospike client, handling any errors.
     *
     * @param groupedKeys An object containing grouped keys for the entities to be deleted {@link TemplateContext}
     *                    containing Aerospike client, converter, and other necessary components
     * @throws AerospikeException if an error occurs during batch delete
     */
    static void deleteGroupedEntitiesByGroupedKeys(GroupedKeys groupedKeys, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));
        deleteAndHandleErrors(templateContext.client, entitiesKeys.getKeys(), true,
            templateContext.exceptionTranslator);
    }

    /**
     * Executes a batch delete operation using the Aerospike client and handles any resulting errors. This method
     * applies a batch policy and performs the delete operation. If the operation fails or if any individual record in
     * the batch fails and {@code skipNonExisting} is false, an appropriate {@link AerospikeException} is thrown.
     *
     * @param client              The Aerospike client instance
     * @param keys                An array of {@link Key} objects representing the records to be deleted
     * @param skipNonExisting     A boolean indicating whether to skip error check for non-existing resulting records
     *                            (allowing {@link ResultCode#OK} and {@link ResultCode#KEY_NOT_FOUND_ERROR}, regardless
     *                            of the record being null)
     * @param exceptionTranslator The translator to convert Aerospike exceptions
     * @throws AerospikeException                                       if an error occurs during batch delete
     * @throws com.aerospike.client.AerospikeException.BatchRecordArray if one of resulting batch records has error
     *                                                                  result code or is null
     */
    private static void deleteAndHandleErrors(IAerospikeClient client, Key[] keys, boolean skipNonExisting,
                                              AerospikeExceptionTranslator exceptionTranslator) {
        BatchResults results;
        try {
            BatchPolicy batchPolicy =
                (BatchPolicy) enrichPolicyWithTransaction(client, client.copyBatchPolicyDefault());
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

    /**
     * Finds and retrieves grouped entities based on a collection of grouped keys. This method converts the
     * {@link GroupedKeys} into {@link EntitiesKeys}, performs a batch read operation using the Aerospike client, and
     * then maps the retrieved {@link Record}s back to {@link GroupedEntities}.
     *
     * @param groupedKeys     An object containing grouped keys for the entities to be found
     * @param templateContext The context containing Aerospike client, mapping utilities, and converter
     * @return A {@link GroupedEntities} object containing the retrieved entities
     * @throws AerospikeException if an error occurs during the batch read
     */
    static GroupedEntities findGroupedEntitiesByGroupedKeys(GroupedKeys groupedKeys, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));
        Record[] records =
            findByKeysUsingQuery(Arrays.stream(entitiesKeys.getKeys()).toList(), null, null, templateContext);

        return MappingUtils.toGroupedEntities(entitiesKeys, records, templateContext.converter);
    }

    /**
     * Finds and retrieves records using an array of {@link Key} objects and an optional {@link Query}. This method
     * allows specifying a subset of bin names to retrieve. It also includes validation for paginated queries with
     * offset.
     *
     * @param keys            A collection of {@link Key} objects representing the records to retrieve
     * @param binNames        An optional array of bin names to retrieve. If null, all bins are retrieved
     * @param query           An optional {@link Query} object to apply filters or pagination
     * @param templateContext The context containing Aerospike client, query engine, and exception translator
     * @return An array of {@link Record} objects matching the criteria
     * @throws IllegalArgumentException if keys or set name are null, or if an invalid paginated query is provided
     * @throws AerospikeException       if an error occurs during the batch read
     */
    private static Record[] findByKeysUsingQuery(Collection<Key> keys, @Nullable String[] binNames,
                                                 @Nullable Query query, TemplateContext templateContext) {
        Assert.notNull(keys, "Keys must not be null!");
        if (isQueryCriteriaNotNull(query)) {
            // Paginated queries with offset and no sorting (i.e. original order)
            // are only allowed for purely id queries, and not for other queries
            verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        }

        if (keys.isEmpty()) {
            return new Record[]{};
        }

        try {
            BatchPolicy batchPolicy = getBatchPolicyFilterExp(query, templateContext);
            return batchReadInChunks(batchPolicy, keys, binNames, templateContext);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    /**
     * Reads records from the database in chunks.
     * <br>
     * This method retrieves records based on a collection of {@link Key}s and an array of bin names. It enriches the
     * provided {@link BatchPolicy} with transaction information and reads records in chunks based on the configured
     * batch read size. If the collection of keys is smaller than or equal to the batch size, or if the batch size is
     * non-positive, all records are read at once without chunking. Otherwise, the keys are processed in batches to
     * optimize performance.
     *
     * @param batchPolicy     The {@link BatchPolicy} to use for the batch read operation
     * @param keys            A {@link Collection} of {@link Key}s representing the records to retrieve
     * @param binNames        An array of bin names to retrieve for each record. If null, all bins will be retrieved
     * @param templateContext The template context providing access to the Aerospike client and data settings
     * @return An array of {@link Record} objects retrieved from the database
     */
    private static Record[] batchReadInChunks(BatchPolicy batchPolicy, Collection<Key> keys, String[] binNames,
                                              TemplateContext templateContext) {
        BatchPolicy batchPolicyEnriched = (BatchPolicy) enrichPolicyWithTransaction(templateContext.client,
            batchPolicy);
        int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchReadSize();

        // For smaller collections of keys or non-positive batchSize, read records straight away without chunking
        if (keys.size() <= batchSize || batchSize <= 0) {
            return batchRead(batchPolicyEnriched, keys.toArray(Key[]::new), binNames, templateContext)
                .toArray(Record[]::new);
        }

        // Pre-allocate result list with estimated capacity
        List<Record> allRecords = new ArrayList<>(keys.size());
        List<Key> keysChunk = new ArrayList<>(batchSize);

        for (Key key : keys) {
            keysChunk.add(key);
            if (keysChunk.size() >= batchSize) {
                // Process chunk and collect results directly
                batchRead(batchPolicyEnriched, keysChunk.toArray(Key[]::new), binNames, templateContext)
                    .forEach(allRecords::add);
                keysChunk.clear();
            }
        }

        // Process any remaining keys
        if (!keysChunk.isEmpty()) {
            batchRead(batchPolicyEnriched, keysChunk.toArray(Key[]::new), binNames, templateContext)
                .forEach(allRecords::add);
        }

        return allRecords.toArray(Record[]::new);
    }

    /**
     * Performs a batch read operation to retrieve records from the database.
     * <br>
     * This method retrieves records based on an array of {@link Key}s and an optional array of bin names. If bin names
     * are provided, only those specific bins will be retrieved for each record. Otherwise, all bins for the specified
     * keys will be retrieved.
     *
     * @param batchPolicy     The {@link BatchPolicy} to use for the batch read operation
     * @param keys            An array of {@link Key}s representing the records to retrieve
     * @param binNames        An array of bin names to retrieve for each record. If null, all bins will be retrieved
     * @param templateContext The template context providing access to the Aerospike client
     * @return A {@link Stream} of {@link Record} objects retrieved from the database
     */
    private static Stream<Record> batchRead(BatchPolicy batchPolicy, Key[] keys, String[] binNames,
                                            TemplateContext templateContext) {
        if (binNames != null) {
            // When target class is given with no bin names (e.g., id projection with sendKeys=true),
            // each BatchRead will be created with readAllBins=false
            List<BatchRead> batchReads = getBatchReadsWithBinNames(keys, binNames);
            templateContext.client.get(batchPolicy, batchReads);
            if (binNames.length == 0) {
                return batchReads.stream()
                    .map(batchRead -> batchRead.record == null
                        ? null
                        // Return records using an empty Map of bins instead of null bins
                        : new Record(Map.of(), batchRead.record.generation, batchRead.record.expiration));
            }
            return batchReads.stream()
                .map(batchRead -> batchRead.record);
        }
        return Arrays.stream(templateContext.client.get(batchPolicy, keys));
    }

    /**
     * Finds records by their IDs using a {@link Query} and returns them as a {@link Stream} without mapping them to
     * specific entity types.
     *
     * @param ids             A collection of IDs for the records to find
     * @param setName         The name of the set where the records are located
     * @param query           The query to apply for filtering or pagination
     * @param templateContext The context containing Aerospike client, and other necessary components
     * @return A {@link Stream} of the found records
     * @throws IllegalArgumentException if the set name is null
     * @throws AerospikeException       if an error occurs during the batch read
     */
    static Stream<Record> findByIdsUsingQueryWithoutMapping(Collection<?> ids, String setName, Query query,
                                                            TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        Stream<Key> keys = MappingUtils.getKeys(ids, setName, templateContext);
        Record[] records = findByKeysUsingQuery(keys.toList(), null, query, templateContext);
        return Arrays.stream(records);
    }

    /**
     * Finds existing records by their IDs and returns them as a {@link Stream} of {@link KeyRecord} objects without
     * mapping them to specific entity types. This method allows specifying a target class to retrieve only relevant bin
     * names.
     *
     * @param ids             A collection of IDs for the records to find
     * @param setName         The name of the set where the records are located
     * @param binNames        Optional bin names to retrieve. If null, all bins are retrieved
     * @param query           Optional query to apply for filtering or pagination
     * @param templateContext The context containing Aerospike client, mapping context, and other necessary components
     * @return A {@link Stream} of {@link KeyRecord} objects representing the found records
     * @throws IllegalArgumentException if IDs are null
     * @throws AerospikeException       if an error occurs during the batch read
     */
    static Stream<KeyRecord> findByIdsWithoutEntityMapping(Collection<?> ids, String setName,
                                                           @Nullable String[] binNames,
                                                           @Nullable Query query, TemplateContext templateContext) {
        Assert.notNull(ids, "Ids must not be null");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        if (ids.isEmpty()) {
            return Stream.empty();
        }

        List<Key> keys = MappingUtils.getKeys(ids, setName, templateContext).toList();
        Record[] records = findByKeysUsingQuery(keys, binNames, query, templateContext);

        return IntStream.range(0, records.length)
            .filter(index -> records[index] != null)
            .mapToObj(index -> new KeyRecord(keys.get(index), records[index]));
    }

    /**
     * Applies a chunked batch write operation to a collection of documents in a reactive manner. This method chunks the
     * input documents into batches and performs the specified operation (save, insert, update, delete) for each batch,
     * returning a {@link Flux} of the processed documents.
     *
     * @param <T>             The type of the documents
     * @param documents       Collection of documents to be written
     * @param setName         The name of the set where the documents will be stored
     * @param operationType   The type of write operation to perform
     * @param templateContext The context containing Aerospike reactive client, converter, and other components
     * @return A {@link Flux} emitting the documents after the batch write operations are complete, or emitting an error
     * if any resulting batch record failed
     */
    static <T> Flux<T> applyReactiveBatchWriteInChunks(Iterable<T> documents, String setName,
                                                       BaseAerospikeTemplate.OperationType operationType,
                                                       TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        return Flux.defer(() -> {
            int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchWriteSize();
            if (batchSize <= 0) {
                // For non-positive batchSize, write records straight away without chunking
                return batchWriteAllDocumentsReactively(iterableToList(documents), setName, operationType,
                    templateContext);
            }
            // Create chunks
            return createNullTolerantBatches(documents, batchSize)
                .concatMap(batch -> batchWriteAllDocumentsReactively(batch, setName, operationType,
                    templateContext));
        });
    }

    /**
     * Performs a batch write operation for a given list of documents reactively. This method prepares the
     * {@link BatchWriteData} for each document based on the specified {@link BaseAerospikeTemplate.OperationType} and
     * then executes the batch operation using the Aerospike reactive client. It also includes error checking and
     * version updating after the batch write completes.
     *
     * @param <T>             The type of the documents
     * @param documents       The list of documents to be written in this batch
     * @param setName         The name of the set where the documents will be stored
     * @param operationType   The type of write operation to perform
     * @param templateContext The context containing Aerospike reactive client, converter, and other components
     * @return A {@link Flux} emitting the documents after the batch write operations are complete, or emitting an error
     * if any resulting batch record failed
     */
    private static <T> Flux<T> batchWriteAllDocumentsReactively(List<T> documents, String setName,
                                                                BaseAerospikeTemplate.OperationType operationType,
                                                                TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

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

                BatchPolicy defaultBatchPolicy =
                    templateContext.reactorClient.getAerospikeClient().copyBatchPolicyDefault();
                return batchWriteReactivelyAndCheckForErrors(
                    defaultBatchPolicy,
                    batchWriteRecords,
                    batchWriteDataList,
                    operationType,
                    templateContext
                );
            } catch (Exception e) {
                return Flux.error(e);
            }
        });
    }

    /**
     * Enriches given {@param batchPolicy} with transaction, executes a reactive batch write operation and checks for
     * errors, updating document versions. This method wraps the reactive client's call, translates any errors, and then
     * proceeds to check for and handle errors and update versions (if applicable) in a reactive chain.
     *
     * @param <T>                The type of the documents
     * @param batchPolicy        The batch policy to apply
     * @param batchWriteRecords  A list of {@link BatchRecord} objects to be written
     * @param batchWriteDataList A list of {@link BatchWriteData} objects, containing the original documents and their
     *                           corresponding batch records
     * @param operationType      The type of write operation performed
     * @param templateContext    The context containing Aerospike reactive client and exception translator
     * @return A {@link Flux} emitting the documents from the processed batch, or emitting an error if any resulting
     * batch record failed
     */
    private static <T> Flux<T> batchWriteReactivelyAndCheckForErrors(BatchPolicy batchPolicy,
                                                                     List<BatchRecord> batchWriteRecords,
                                                                     List<BatchWriteData<T>> batchWriteDataList,
                                                                     BaseAerospikeTemplate.OperationType operationType,
                                                                     TemplateContext templateContext) {
        return enrichPolicyWithTransaction(templateContext.reactorClient, batchPolicy)
            .flatMap(batchPolicyEnriched ->
                templateContext.reactorClient.operate((BatchPolicy) batchPolicyEnriched, batchWriteRecords))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator))
            .flatMap(ignore -> checkForErrorsAndUpdateVersionForReactive(batchWriteDataList, batchWriteRecords,
                operationType, templateContext))
            .flux()
            .flatMapIterable(list -> list.stream().map(BatchWriteData::document).toList());
    }

    /**
     * Checks for errors in the results of a batch write operation and updates document versions (if applicable). This
     * method operates within a reactive {@link Mono} context. It identifies errors and returns a
     * {@link Mono#error(Throwable)} if errors are found, otherwise a {@link Mono#just(Object)} containing the batch
     * write data.
     *
     * @param <T>                The type of the documents
     * @param batchWriteDataList A list of {@link BatchWriteData} objects, containing the original documents and their
     *                           corresponding batch records
     * @param batchWriteRecords  A list of {@link BatchRecord} objects returned by the client after the batch operation
     * @param operationType      The type of write operation performed
     * @param templateContext    The context containing the exception translator and other necessary components
     * @return A {@link Mono} that completes successfully with the list of {@link BatchWriteData} if no errors, or emits
     * {@link AerospikeException} if any resulting batch record failed, or {@link OptimisticLockingFailureException} if
     * version mismatch (CAS error) occurred
     */
    private static <T> Mono<List<BatchWriteData<T>>> checkForErrorsAndUpdateVersionForReactive(List<BatchWriteData<T>> batchWriteDataList,
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
                return Mono.error(new OptimisticLockingFailureException(
                    "Failed to %s the record with ID '%s' due to versions mismatch"
                        .formatted(operationType, casErrorDocumentId), null));
            }
            AerospikeException e = new AerospikeException("Errors during batch " + operationType);
            return Mono.error(
                new AerospikeException.BatchRecordArray(batchWriteRecords.toArray(BatchRecord[]::new), e));
        }

        return Mono.just(batchWriteDataList);
    }

    /**
     * Deletes documents by their IDs in a chunked batches reactively. This method takes an iterable of IDs, chunks them
     * into batches, and then performs the deletion for each batch reactively, returning a {@link Mono} that completes
     * when all deletions are done.
     *
     * @param ids             An iterable collection of document IDs to delete
     * @param setName         The name of the set from which to delete documents
     * @param skipNonExisting A boolean indicating whether to skip error check for non-existing resulting records
     *                        (allowing {@link ResultCode#OK} and {@link ResultCode#KEY_NOT_FOUND_ERROR}, regardless of
     *                        the record being null)
     * @param templateContext The context containing Aerospike reactive client, converter, and other components
     * @return A {@link Mono<Void>} that completes when all deletions are finished if no errors, or emits an error if
     * any resulting batch record failed
     * @throws IllegalArgumentException if the set name is null
     */
    static Mono<Void> deleteByIdsReactively(Iterable<?> ids, String setName, boolean skipNonExisting,
                                            TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        if (isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return Mono.empty();
        }

        int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchWriteSize();
        if (batchSize <= 0) {
            // For non-positive batchSize, delete records straight away without chunking
            return doDeleteByIdsReactively(iterableToList(ids), setName, skipNonExisting, templateContext);
        }

        List<Object> idsList = new ArrayList<>();
        List<Mono<Void>> deleteResults = new ArrayList<>();
        for (Object id : ids) {
            if (batchSizeMatch(batchSize, idsList.size())) {
                deleteResults.add(doDeleteByIdsReactively(new ArrayList<>(idsList), setName, skipNonExisting,
                    templateContext));
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            deleteResults.add(doDeleteByIdsReactively(new ArrayList<>(idsList), setName, skipNonExisting,
                templateContext));
        }

        return Flux.concat(Flux.fromIterable(deleteResults)).then();
    }

    /**
     * Performs reactive batch deletion of documents by their IDs. This method converts the provided IDs into Aerospike
     * {@link Key} objects and then calls
     * {@link #batchDeleteReactivelyAndCheckForErrors(IAerospikeReactorClient, Key[], boolean,
     * AerospikeExceptionTranslator)} to execute the deletion and handle any errors reactively.
     *
     * @param ids             A collection of document IDs to delete
     * @param setName         The name of the set from which to delete documents
     * @param skipNonExisting A boolean indicating whether to skip error check for non-existing resulting records
     *                        (allowing {@link ResultCode#OK} and {@link ResultCode#KEY_NOT_FOUND_ERROR}, regardless of
     *                        the record being null)
     * @param templateContext The context containing Aerospike reactive client, converter, and other components
     * @return A {@link Mono<Void>} that completes when the deletion is finished if no errors, or emits an error if a
     * batch record within the results failed
     * @throws IllegalArgumentException if the set name is null
     */
    private static Mono<Void> doDeleteByIdsReactively(Collection<?> ids, String setName, boolean skipNonExisting,
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

    /**
     * Executes a reactive batch delete operation and handles any resulting errors. If the operation fails or if any
     * individual record in the result fails or is null, an appropriate {@link AerospikeException} is emitted as an
     * error.
     *
     * @param reactorClient       The instance of the Aerospike reactive client
     * @param keys                An array of {@link Key} objects representing the records to be deleted
     * @param skipNonExisting     A boolean indicating whether to skip error check for non-existing resulting records
     *                            (allowing {@link ResultCode#OK} and {@link ResultCode#KEY_NOT_FOUND_ERROR}, regardless
     *                            of the record being null)
     * @param exceptionTranslator The translator to convert Aerospike exceptions
     * @return A {@link Mono<Void>} that completes successfully if no errors, or emits {@link AerospikeException} if any
     * resulting batch record failed
     */
    private static Mono<Void> batchDeleteReactivelyAndCheckForErrors(IAerospikeReactorClient reactorClient, Key[] keys,
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

        return enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.delete((BatchPolicy) batchPolicy, null, keys))
            .onErrorMap(e -> ExceptionUtils.translateError(e, exceptionTranslator))
            .flatMap(checkForErrors);
    }

    /**
     * Deletes entities reactively based on a collection of grouped keys. This method first converts the
     * {@link GroupedKeys} into {@link EntitiesKeys} and then performs a batch delete operation using the Aerospike
     * reactive client, handling any errors.
     *
     * @param groupedKeys     An object containing grouped keys for the entities to be deleted
     * @param templateContext The context containing required template dependencies
     * @return A {@link Mono<Void>} that completes when the deletion is finished, or emits {@link AerospikeException} if
     * any resulting batch record failed
     */
    static Mono<Void> deleteEntitiesByGroupedKeysReactively(GroupedKeys groupedKeys, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));

        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
            .copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.delete((BatchPolicy) batchPolicy, null, entitiesKeys.getKeys()))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));

        return batchDeleteReactivelyAndCheckForErrors(reactorClient, entitiesKeys.getKeys(), true,
            templateContext.exceptionTranslator);
    }

    /**
     * Finds and retrieves grouped entities reactively based on a collection of grouped keys. This method converts the
     * {@link GroupedKeys} into {@link EntitiesKeys}, performs a batch read operation using the Aerospike reactive
     * client, and then maps the retrieved {@link Record}s back to {@link GroupedEntities}.
     *
     * @param batchPolicy     The batch policy to apply to the operation
     * @param groupedKeys     An object containing grouped keys for the entities to be found
     * @param templateContext The context containing Aerospike reactive client, mapping utilities, and converter
     * @return A {@link Mono} emitting a {@link GroupedEntities} object containing the retrieved entities, or emitting
     * an error if reading or mapping failed
     */
    static Mono<GroupedEntities> findGroupedEntitiesByGroupedKeysReactively(BatchPolicy batchPolicy,
                                                                            GroupedKeys groupedKeys,
                                                                            TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        EntitiesKeys entitiesKeys = EntitiesKeys.of(MappingUtils.toEntitiesKeyMap(groupedKeys, templateContext));

        return batchReadInChunksReactively(batchPolicy, entitiesKeys.getKeys(), null, templateContext)
            .collectList()
            .map(keyRecordsList -> MappingUtils.toGroupedEntities(
                    entitiesKeys,
                    getRecordsStream(keyRecordsList).toArray(Record[]::new),
                    templateContext.converter
                )
            )
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    private static Stream<Record> getRecordsStream(List<KeyRecord> keyRecordsList) {
        return keyRecordsList.stream().map(keyRecord -> keyRecord.record);
    }

    /**
     * Retrieves {@link BatchPolicy} with a filter expression applied, suitable for reactive queries. If the provided
     * {@link Query} has criteria, a filter expression is built and applied to a default batch policy. Otherwise, a
     * default batch policy is returned.
     *
     * @param query           The {@link Query} object that may contain criteria
     * @param templateContext The context containing the Aerospike reactive client and query engine
     * @return A {@link BatchPolicy} with or without a filter expression, based on the query
     */
    static BatchPolicy getBatchPolicyForReactive(Query query, TemplateContext templateContext) {
        if (isQueryCriteriaNotNull(query)) {
            BatchPolicy batchPolicy = templateContext.reactorClient.getAerospikeClient().copyBatchPolicyDefault();
            Qualifier qualifier = query.getCriteriaObject();
            batchPolicy.filterExp = templateContext.reactorQueryEngine.getFilterExpressionsBuilder().build(qualifier);
            return batchPolicy;
        }
        return templateContext.reactorClient.getAerospikeClient().copyBatchPolicyDefault();
    }

    /**
     * Performs chunked batch read and retrieves a {@link Flux} of {@link KeyRecord} objects. The method enriches the
     * given batch policy with transaction details. If a target class is provided, only specific bin names based on this
     * class are retrieved.
     *
     * @param batchPolicy     The batch policy to apply to the operation. Can be null, in which case a default is used
     * @param keys            An array of Aerospike client {@link Key}s to retrieve
     * @param targetClass     If provided, only bins relevant to this class are retrieved; can be {@code null}
     * @param templateContext The context containing Aerospike reactive client, mapping context, and other components
     * @return A {@link Flux} of {@link KeyRecord}s
     */
    static Flux<KeyRecord> batchReadInChunksReactively(BatchPolicy batchPolicy, Key[] keys,
                                                       @Nullable Class<?> targetClass,
                                                       TemplateContext templateContext) {
        Mono<Policy> enrichedPolicyMono = enrichPolicyWithTransaction(templateContext.reactorClient, batchPolicy);
        int batchSize = templateContext.converter.getAerospikeDataSettings().getBatchReadSize();

        return enrichedPolicyMono
            .flatMapMany(batchPolicyEnriched -> {
                if (batchSize <= 0) {
                    // Process all keys in one go without chunking if batchSize value is non-positive
                    return batchReadReactively((BatchPolicy) batchPolicyEnriched, keys, targetClass, templateContext)
                        .flatMapIterable(BatchUtils::keysRecordsToList);
                } else {
                    // Read by keys in chunks
                    return Flux.fromArray(keys)
                        .buffer(batchSize)
                        .flatMapSequential(keyList -> {
                            // Convert each chunk back to array and process
                            Key[] keysChunk = keyList.toArray(new Key[0]);
                            return batchReadReactively((BatchPolicy) batchPolicyEnriched, keysChunk, targetClass,
                                templateContext)
                                .flatMapIterable(BatchUtils::keysRecordsToList);
                        }, 1); // Use maximal concurrency of 1 to ensure the chunks are processed in order
                }
            });
    }

    /**
     * Converts a {@link KeysRecords} object into an {@link Iterable} of {@link KeyRecord} while preserving the original
     * order.
     * <br>
     * This method iterates through the arrays of keys and records within the provided {@link KeysRecords} object and
     * creates a new {@link KeyRecord} for each corresponding key-record pair, maintaining their original sequence.
     *
     * @param keysRecords The {@link KeysRecords} object containing arrays of keys and records
     * @return An {@link Iterable} of {@link KeyRecord} objects
     */
    private static Iterable<KeyRecord> keysRecordsToList(KeysRecords keysRecords) {
        // Create KeyRecord objects directly while preserving the original order
        List<KeyRecord> result = new ArrayList<>(keysRecords.keys.length);
        for (int i = 0; i < keysRecords.keys.length; i++) {
            result.add(new KeyRecord(keysRecords.keys[i], keysRecords.records[i]));
        }
        return result;
    }

    /**
     * Performs a reactive batch read operation.
     * <br>
     * This method asynchronously retrieves records using a provided {@link BatchPolicy}, an array of {@link Key}s, and
     * an optional target class. If a target class is provided, it retrieves specific bins based on that class.
     * Otherwise, it retrieves all bins.
     *
     * @param batchPolicy     The {@link BatchPolicy} to use for the batch read operation
     * @param keys            An array of {@link Key}s representing the records to retrieve
     * @param targetClass     The {@link Class} of the target entity, used to determine bins, can be {@code null}
     * @param templateContext The template context providing access to the reactive client and mapping context
     * @return A {@link Mono} that emits a {@link KeysRecords} object containing the retrieved keys and records
     */
    private static Mono<KeysRecords> batchReadReactively(BatchPolicy batchPolicy, Key[] keys,
                                                         @Nullable Class<?> targetClass,
                                                         TemplateContext templateContext) {
        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        String[] binNames = getBinNamesFromTargetClassOrNull(null, targetClass, templateContext.mappingContext);
        // When target class is given with no bin names (e.g., id projection with sendKeys=true),
        // each BatchRead will be created with readAllBins=false
        if (binNames != null) {
            return reactorClient.get(batchPolicy, getBatchReadsWithBinNames(keys, binNames))
                .flatMap(batchReads -> batchReadsToKeysRecords(keys, batchReads, binNames.length == 0));
        }
        return reactorClient.get(batchPolicy, keys);
    }

    /**
     * Converts an array of {@link Key}s and a list of {@link BatchRead} objects into a {@link Mono} of
     * {@link KeysRecords}.
     *
     * @param keys         An array of {@link Key}s
     * @param batchReads   A {@link List} of {@link BatchRead} objects containing the results of the batch read
     * @param areEmptyBins {@code true} if bin names are empty
     * @return A {@link Mono} that emits a {@link KeysRecords} object
     */
    private static Mono<KeysRecords> batchReadsToKeysRecords(Key[] keys, List<BatchRead> batchReads,
                                                             boolean areEmptyBins) {
        Record[] records = batchReads.stream()
            .map(batchRead -> {
                if (areEmptyBins) {
                    return batchRead.record == null
                        ? null
                        // Return records using an empty Map of bins instead of null bins
                        : new Record(Map.of(), batchRead.record.generation, batchRead.record.expiration);
                }
                return batchRead.record;
            })
            .toArray(Record[]::new);

        return Mono.just(new KeysRecords(keys, records));
    }

    /**
     * Creates a list of {@link BatchRead} objects with specified bin names for each key.
     *
     * @param keys     An array of {@link Key}s, for each of them a {@link BatchRead} object is created
     * @param binNames An array of bin names to include in each {@link BatchRead} object
     * @return A {@link List} of {@link BatchRead} objects
     */
    private static List<BatchRead> getBatchReadsWithBinNames(Key[] keys, String[] binNames) {
        if (binNames == null || binNames.length == 0) {
            return Arrays.stream(keys).map(key -> new BatchRead(key, false))
                .collect(Collectors.toCollection(() -> new ArrayList<>(keys.length)));
        }
        return Arrays.stream(keys).map(key -> new BatchRead(key, binNames))
            .collect(Collectors.toCollection(() -> new ArrayList<>(keys.length)));
    }

    /**
     * Creates a {@link BatchWriteData} object for a "save" operation. This method prepares the necessary
     * {@link BatchWritePolicy} and {@link Operation}s for saving a document. It handles versioning property to
     * determine the policy and operations. For documents with a version property, it mimics a REPLACE behavior by
     * deleting bins first.
     *
     * @param <T>             The type of the document
     * @param document        The document to be saved
     * @param setName         The name of the set where the document will be stored
     * @param templateContext The context containing mapping context, batch write policy default, and other components
     * @return A {@link BatchWriteData} object configured for a save operation
     * @throws IllegalArgumentException if the document is null
     */
    private static <T> BatchWriteData<T> getBatchWriteForSave(T document, String setName,
                                                              TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);

        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy = expectGenerationCasAwareBatchPolicy(data, templateContext.batchWritePolicyDefault);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = TemplateUtils.getPutAndGetHeaderOperations(data, true);
        } else {
            policy = ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE,
                templateContext.batchWritePolicyDefault);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = operations(data.getBinsAsArray(), Operation::put, Operation.array(Operation.delete()));
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    /**
     * Creates a {@link BatchWriteData} object for an "insert" operation. This method prepares the necessary
     * {@link BatchWritePolicy} and {@link Operation}s for inserting a document. It ensures that the record exists only
     * if it's new (CREATE_ONLY).
     *
     * @param <T>             The type of the document
     * @param document        The document to be inserted
     * @param setName         The name of the set where the document will be stored
     * @param templateContext The context containing mapping context, batch write policy default, and other components
     * @return A {@link BatchWriteData} object configured for an insert operation
     * @throws IllegalArgumentException if the document is null
     */
    private static <T> BatchWriteData<T> getBatchWriteForInsert(T document, String setName,
                                                                TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        BatchWritePolicy policy =
            ignoreGenerationBatchPolicy(data, RecordExistsAction.CREATE_ONLY, templateContext.batchWritePolicyDefault);
        Operation[] operations;
        if (entity.hasVersionProperty()) {
            operations = TemplateUtils.getPutAndGetHeaderOperations(data, false);
        } else {
            operations = operations(data.getBinsAsArray(), Operation::put);
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    /**
     * Creates a {@link BatchWriteData} object for an 'update' operation. This method prepares the necessary
     * {@link BatchWritePolicy} and {@link Operation}s for updating a document. It ensures that the record is updated
     * only if it already exists (UPDATE_ONLY). For documents with a version property, it mimics a REPLACE_ONLY behavior
     * by deleting bins first.
     *
     * @param <T>             The type of the document
     * @param document        The document to be updated
     * @param setName         The name of the set where the document is located
     * @param templateContext The context containing mapping context, batch write policy default, and other components
     * @return A {@link BatchWriteData} object configured for an update operation
     * @throws IllegalArgumentException if the document is null
     */
    private static <T> BatchWriteData<T> getBatchWriteForUpdate(T document, String setName,
                                                                TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy =
                expectGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY,
                    templateContext.batchWritePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = TemplateUtils.getPutAndGetHeaderOperations(data, true);
        } else {
            policy = ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.batchWritePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = Stream.concat(Stream.of(Operation.delete()), data.getBins().stream()
                .map(Operation::put)).toArray(Operation[]::new);
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    /**
     * Creates a {@link BatchWriteData} object for a 'delete' operation. This method prepares the necessary
     * {@link BatchWritePolicy} and {@link Operation}s for deleting a document. It considers the version property for
     * policy configuration.
     *
     * @param <T>             The type of the document
     * @param document        The document to be deleted
     * @param setName         The name of the set where the document is located
     * @param templateContext The context containing mapping context, batch write policy default, and other components
     * @return A {@link BatchWriteData} object configured for a delete operation
     * @throws IllegalArgumentException if the document is null
     */
    private static <T> BatchWriteData<T> getBatchWriteForDelete(T document, String setName,
                                                                TemplateContext templateContext) {
        Assert.notNull(document, "Document must not be null!");

        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        AerospikeWriteData data = TemplateUtils.writeData(document, setName, templateContext);

        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy = expectGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.batchWritePolicyDefault);
        } else {
            policy = ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.batchWritePolicyDefault);
        }
        Operation[] operations = Operation.array(Operation.delete());

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    /**
     * Checks if the current size of a batch matches the defined batch size. This is used to determine when a batch is
     * full and should be processed.
     *
     * @param batchSize   The maximum size of a batch
     * @param currentSize The current number of items in the batch
     * @return {@code true} if the current size equals the batch size and the batch size is positive, {@code false}
     * otherwise
     */
    static boolean batchSizeMatch(int batchSize, int currentSize) {
        return batchSize > 0 && currentSize == batchSize;
    }

    /**
     * Determines if given {@link BatchRecord} can be considered failed. This method checks the {@code resultCode} of
     * the batch record. If {@code skipNonExisting} is true, a {@link ResultCode#KEY_NOT_FOUND_ERROR} is not considered
     * a failure, regardless of the record being null. Otherwise, any result code other than {@link ResultCode#OK} or
     * the record being null indicates a failure.
     *
     * @param batchRecord     The {@link BatchRecord} to check
     * @param skipNonExisting A boolean indicating whether to skip error check for
     *                        {@link ResultCode#KEY_NOT_FOUND_ERROR} regardless of the record being null
     * @return {@code true} if the batch record indicates as failed, {@code false} otherwise
     */
    private static boolean batchRecordFailed(BatchRecord batchRecord, boolean skipNonExisting) {
        int resultCode = batchRecord.resultCode;
        if (skipNonExisting) {
            return resultCode != ResultCode.OK && resultCode != ResultCode.KEY_NOT_FOUND_ERROR;
        }
        return resultCode != ResultCode.OK || batchRecord.record == null;
    }

    /**
     * Creates batches from an iterable source, tolerating null values. Each batch will have at most batchSize
     * elements.
     *
     * @param source    The source iterable containing elements to batch
     * @param batchSize The maximal size of each batch
     * @return A Flux emitting lists of batched elements, or an error in case of an exception found
     */
    private static <T> Flux<List<T>> createNullTolerantBatches(Iterable<? extends T> source, int batchSize) {
        return Flux.create(sink -> {
            try {
                List<T> currentBatch = new ArrayList<>();

                for (T item : source) {
                    // Add item to the current batch (even if null)
                    currentBatch.add(item);

                    // When we hit batch size, emit the batch and start a new one
                    if (batchSizeMatch(batchSize, currentBatch.size())) {
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

    /**
     * Finds entities by their IDs without post-processing.
     *
     * <p>This method retrieves a {@link Stream} of entities based on an iterable of IDs, an entity class,
     * an optional target class, a set name, and an optional query. It maps the records to the entity or target class
     * without applying further post-processing.</p>
     *
     * @param <T>             The type of the entity
     * @param <S>             The type of the target class to which the entities will be mapped
     * @param ids             An {@link Iterable} of IDs of the entities to find
     * @param entityClass     The {@link Class} of the entity
     * @param targetClass     The {@link Class} to which the retrieved entities will be mapped, can be {@code null}
     * @param setName         The name of the set where the records are stored
     * @param query           A {@link Query} to apply filtering or criteria to the search, can be {@code null}
     * @param templateContext The template context to be used
     * @return A {@link Stream} of entities, mapped to the target class, without further post-processing
     */
    static <T, S> Stream<?> findByIdsWithoutPostProcessing(Iterable<?> ids, Class<T> entityClass,
                                                           @Nullable Class<S> targetClass, String setName,
                                                           @Nullable Query query, TemplateContext templateContext) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        List<Key> keys = MappingUtils.getKeys(iterableToList(ids), setName, templateContext).toList();
        String[] binNames = getBinNamesFromTargetClassOrNull(entityClass, targetClass, templateContext.mappingContext);
        Record[] records = findByKeysUsingQuery(keys, binNames, query, templateContext);

        return IntStream.range(0, keys.size())
            .mapToObj(index -> mapToEntity(keys.get(index), getTargetClass(entityClass, targetClass), records[index],
                templateContext.converter));
    }

    /**
     * Finds records reactively by their IDs using a query without entity mapping.
     *
     * <p>This method retrieves a {@link Flux} of {@link KeyRecord} instances based on a collection of IDs, a set name,
     * and an optional query. It performs a batch read in chunks reactively and does not map the results to specific
     * entity classes.</p>
     *
     * @param ids             A {@link Collection} of IDs of the records to find
     * @param setName         The name of the set where the records are stored
     * @param query           A {@link Query} to apply filtering or criteria to the search, can be {@code null}
     * @param templateContext The template context to be used
     * @return A {@link Flux} of {@link KeyRecord} instances
     */
    static Flux<KeyRecord> findByIdsUsingQueryWithoutEntityMappingReactively(Collection<?> ids, String setName,
                                                                             @Nullable Query query,
                                                                             TemplateContext templateContext) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }
        BatchPolicy batchPolicy = getBatchPolicyForReactive(query, templateContext);
        Key[] keys = getKeys(iterableToList(ids), setName, templateContext).toArray(Key[]::new);

        return batchReadInChunksReactively(batchPolicy, keys, null, templateContext);
    }

    /**
     * Finds entities reactively by their IDs without post-processing.
     *
     * <p>This method retrieves a {@link Flux} of entities based on a collection of IDs, an entity class,
     * an optional target class, a set name, and an optional query. It performs a batch read in chunks reactively and
     * maps the raw results to the target or entity class without applying further post-processing.</p>
     *
     * @param <T>             The type of the entity
     * @param <S>             The type of the target class to which the entities will be mapped
     * @param ids             A {@link Collection} of IDs of the entities to find
     * @param entityClass     The {@link Class} of the entity
     * @param targetClass     The {@link Class} to which the retrieved entities will be mapped, can be {@code null}
     * @param setName         The name of the set where the records are stored
     * @param query           A {@link Query} to apply initial filtering or criteria to the search, can be {@code null}
     * @param templateContext The template context to be used
     * @return A {@link Flux} of entities, mapped to the entity or target class, without further post-processing
     */
    static <T, S> Flux<?> findByIdsWithoutPostProcessingReactively(Collection<?> ids, Class<T> entityClass,
                                                                   @Nullable Class<S> targetClass, String setName,
                                                                   @Nullable Query query,
                                                                   TemplateContext templateContext) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }
        BatchPolicy batchPolicy = getBatchPolicyForReactive(query, templateContext);
        Class<?> targetType = getTargetClass(entityClass, targetClass);
        Key[] keys = getKeys(iterableToList(ids), setName, templateContext).toArray(Key[]::new);

        return batchReadInChunksReactively(batchPolicy, keys, targetType, templateContext)
            .flatMap(keyRecord -> MappingUtils.mapToEntityReactively(keyRecord.key, targetType, keyRecord.record,
                templateContext.converter));
    }
}
