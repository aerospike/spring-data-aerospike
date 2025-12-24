package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.KeyRecordIterator;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.util.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.StreamUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.MappingUtils.getBinNamesFromTargetClassOrNull;
import static org.springframework.data.aerospike.core.MappingUtils.getKeys;
import static org.springframework.data.aerospike.core.ValidationUtils.verifyUnsortedWithOffset;
import static org.springframework.data.aerospike.core.MappingUtils.convertIfNecessary;
import static org.springframework.data.aerospike.core.QualifierUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.core.QualifierUtils.getIdValue;
import static org.springframework.data.aerospike.core.MappingUtils.getBinNamesFromTargetClass;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;
import static org.springframework.data.aerospike.query.QualifierUtils.isQueryCriteriaNotNull;
import static org.springframework.data.aerospike.util.Utils.iterableToList;

/**
 * A utility class providing methods used by {@link AerospikeTemplate} and {@link ReactiveAerospikeTemplate}.
 */
@Slf4j
public class TemplateUtils {

    private TemplateUtils() {
        throw new UnsupportedOperationException("Utility class TemplateUtils cannot be instantiated");
    }

    /**
     * Retrieves a {@link ConvertingPropertyAccessor} for a given entity and source object. This accessor allows setting
     * properties on the source object with type conversion, leveraging the provided {@link MappingAerospikeConverter}.
     *
     * @param <T>       The type of the source object
     * @param entity    The {@link AerospikePersistentEntity} representing the entity's metadata
     * @param source    The source object whose properties are to be accessed
     * @param converter The {@link MappingAerospikeConverter} used for type conversions
     * @return A {@link ConvertingPropertyAccessor} instance that can be used to set properties on the source object
     * with appropriate type conversions
     */
    private static <T> ConvertingPropertyAccessor<T> getPropertyAccessor(AerospikePersistentEntity<?> entity, T source,
                                                                         MappingAerospikeConverter converter) {
        PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
        return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
    }

    /**
     * Updates the version property of a document based on the generation of a new Aerospike record. This method is
     * typically called after a successful write operation to synchronize the document's in-memory version with the
     * actual generation value stored in the Aerospike database.
     *
     * @param <T>             The type of the document
     * @param document        The document object whose version needs to be updated. This object will be modified
     * @param newAeroRecord   The new {@link Record} returned from Aerospike after a write operation, which contains the
     *                        updated generation value
     * @param templateContext The {@link TemplateContext} providing necessary services like mapping context and
     *                        converter to access and update the version property
     * @return The updated document object with its version property set to the {@code newAeroRecord.generation}
     * @throws IllegalStateException if the entity does not have a required version property defined, which is essential
     *                               for optimistic locking
     */
    static <T> T updateVersion(T document, Record newAeroRecord, TemplateContext templateContext) {
        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());
        ConvertingPropertyAccessor<T> propertyAccessor =
            getPropertyAccessor(entity, document, templateContext.converter);
        AerospikePersistentProperty versionProperty = entity.getRequiredVersionProperty();
        propertyAccessor.setProperty(versionProperty, newAeroRecord.generation);
        return document;
    }

    /**
     * Logs a debug message indicating that a specific set of items is empty.
     *
     * @param log              The {@link Logger} instance to use for logging the debug message
     * @param itemsDescription A descriptive string identifying the items that are empty
     */
    static void logEmptyItems(Logger log, String itemsDescription) {
        log.debug("{} are empty", itemsDescription);
    }

    /**
     * Attempts to delete a record from Aerospike with optimistic locking (version checking). This method uses a
     * {@link WritePolicy} configured with {@link RecordExistsAction#UPDATE_ONLY} and expects a specific generation to
     * be present. If the record's generation in the database does not match the expected version in {@code data}, an
     * {@link AerospikeException} is caught, translated into a Spring Data
     * {@link org.springframework.dao.OptimisticLockingFailureException} and rethrown.
     *
     * @param data            The {@link AerospikeWriteData} object containing the {@link Key} of the record to be
     *                        deleted and its expected version for optimistic locking
     * @param templateContext The {@link TemplateContext} providing the Aerospike client, default write policy, and an
     *                        exception translator
     * @return {@code true} if the record was successfully deleted with the matching version; {@code false} otherwise
     * (though typically an exception would be thrown on mismatch)
     * @throws org.springframework.dao.OptimisticLockingFailureException if a version mismatch occurs during the delete
     *                                                                   operation (CAS error)
     * @throws org.springframework.dao.DataAccessException               for other Aerospike-related errors that occur
     *                                                                   during deletion
     */
    static boolean doDeleteWithVersionAndHandleCasError(AerospikeWriteData data,
                                                        TemplateContext templateContext) {
        try {
            WritePolicy writePolicy =
                PolicyUtils.expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                    templateContext.writePolicyDefault);
            //noinspection DataFlowIssue
            writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, writePolicy);
            return templateContext.client.delete(writePolicy, data.getKey());
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateCasException(e, "Failed to delete record due to versions mismatch",
                templateContext.exceptionTranslator);
        }
    }

    /**
     * Attempts to delete a record from Aerospike, ignoring any version checks. This method configures the
     * {@link WritePolicy} to disregard the generation (version) of the record during deletion. Any
     * {@link AerospikeException} encountered during the deletion process is caught, translated into a Spring Data
     * {@link org.springframework.dao.DataAccessException} and rethrown.
     *
     * @param data            The {@link AerospikeWriteData} object containing the {@link Key} of the record to be
     *                        deleted
     * @param templateContext The {@link TemplateContext} providing the Aerospike client, default write policy, and an
     *                        exception translator
     * @return {@code true} if the record was successfully deleted; {@code false} otherwise
     * @throws org.springframework.dao.DataAccessException for any Aerospike-related errors that occur during deletion
     */
    static boolean doDeleteIgnoreVersionAndTranslateError(AerospikeWriteData data, TemplateContext templateContext) {
        try {
            WritePolicy policy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.writePolicyDefault);
            WritePolicy writePolicy =
                (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, policy);
            return templateContext.client.delete(writePolicy, data.getKey());
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    /**
     * Retrieves a record based on the entity's configuration, including "touch on read" behavior. If "touch on read" is
     * enabled for the entity, this method attempts to update the record's expiration time upon retrieval. It also
     * handles optional query filters.
     *
     * @param entity          The {@link AerospikePersistentEntity} representing the entity's metadata, used to
     *                        determine "touch on read" behavior and expiration property
     * @param key             The {@link Key} of the record to retrieve
     * @param query           An optional {@link Query} object that can contain filter expressions to be applied during
     *                        the retrieval. Can be {@code null}
     * @param templateContext The {@link TemplateContext} providing the Aerospike client, default read policy, and query
     *                        engine
     * @return The {@link Record} retrieved from Aerospike, or {@code null} if the record is not found
     * @throws IllegalStateException                       if "touch on read" is enabled for the entity but the entity
     *                                                     does not have an expiration property defined, which is
     *                                                     required for touching
     * @throws org.springframework.dao.DataAccessException for any Aerospike-related errors that occur during retrieval
     */
    static Record getRecord(AerospikePersistentEntity<?> entity, Key key, @Nullable Query query,
                            TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        Record aeroRecord;
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = touchAndGet(key, entity.getExpiration(), templateContext, null, null);
        } else {
            Policy policy = PolicyUtils.enrichPolicyWithTransaction(
                templateContext.client,
                PolicyUtils.getPolicyFilterExpOrDefault(templateContext.client, templateContext.queryEngine, query));
            aeroRecord = templateContext.client.get(policy, key);
        }
        return aeroRecord;
    }

    /**
     * Retrieves a record, potentially touching it (updating TTL), and then maps the retrieved {@link Record} to an
     * instance of the specified {@code targetClass}. This method handles "touch on read" logic, applies optional query
     * filters, and retrieves only the necessary bins based on the target class's mapping.
     *
     * @param <T>             The target class type to which the record will be mapped
     * @param entity          The {@link AerospikePersistentEntity} representing the entity's metadata, used for "touch
     *                        on read" and bin name resolution
     * @param key             The {@link Key} of the record to retrieve
     * @param targetClass     The {@link Class} to which the Aerospike record should be mapped
     * @param query           An optional {@link Query} object that can contain filter expressions to be applied during
     *                        the retrieval. Can be {@code null}
     * @param templateContext The {@link TemplateContext} providing the Aerospike client, policies, mapping context, and
     *                        converter for mapping the record
     * @return An object of type {@code T} mapped from the Aerospike record, or {@code null} if the record is not found
     * @throws IllegalStateException                       if "touch on read" is enabled for the entity but the entity
     *                                                     does not have an expiration property defined
     * @throws org.springframework.dao.DataAccessException for any Aerospike-related errors that occur during retrieval
     *                                                     or mapping
     */
    static <T> Object getRecordMapToTargetClass(AerospikePersistentEntity<?> entity, Key key, Class<T> targetClass,
                                                @Nullable Query query, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        Record aeroRecord;
        String[] binNames = getBinNamesFromTargetClass(targetClass, templateContext.mappingContext);
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = touchAndGet(key, entity.getExpiration(), templateContext, binNames, query);
        } else {
            Policy policy = PolicyUtils.enrichPolicyWithTransaction(
                templateContext.client,
                PolicyUtils.getPolicyFilterExpOrDefault(templateContext.client, templateContext.queryEngine, query)
            );
            aeroRecord = templateContext.client.get(policy, key, binNames);
        }
        return MappingUtils.mapToEntity(key, targetClass, aeroRecord, templateContext.converter);
    }

    /**
     * Touches a record (updates its expiration time) and retrieves its contents.
     *
     * @param key             The {@link Key} of the record to retrieve and touch
     * @param expiration      The new expiration time for the record (in seconds from now)
     * @param templateContext The {@link TemplateContext} providing the Aerospike client
     * @param binNames        An optional array of bin names to retrieve. If {@code null}, all bins are retrieved
     * @param query           An optional {@link Query} to apply a filter expression during retrieval. Can be
     *                        {@code null}
     * @return The {@link Record} retrieved
     */
    private static Record touchAndGet(Key key, int expiration, TemplateContext templateContext,
                                      @Nullable String[] binNames, @Nullable Query query) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        WritePolicyBuilder writePolicyBuilder =
            WritePolicyBuilder.builder(templateContext.client.copyWritePolicyDefault()).expiration(expiration);

        if (isQueryCriteriaNotNull(query)) {
            Qualifier qualifier = query.getCriteriaObject();
            writePolicyBuilder.filterExp(templateContext.queryEngine.getFilterExpressionsBuilder().build(qualifier));
        }
        WritePolicy writePolicy =
            (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, writePolicyBuilder.build());

        try {
            if (binNames == null || binNames.length == 0) {
                return templateContext.client.operate(writePolicy, key, Operation.touch(), Operation.get());
            } else {
                Operation[] operations = new Operation[binNames.length + 1];
                operations[0] = Operation.touch();

                for (int i = 1; i < operations.length; i++) {
                    operations[i] = Operation.get(binNames[i - 1]);
                }
                return templateContext.client.operate(writePolicy, key, operations);
            }
        } catch (AerospikeException aerospikeException) {
            if (aerospikeException.getResultCode() == ResultCode.KEY_NOT_FOUND_ERROR) {
                return null;
            }
            throw aerospikeException;
        }
    }

    /**
     * Finds records of a specified type within a given set. This method maps the retrieved key records to entities of
     * the target class.
     *
     * @param <T>             The type of the target class
     * @param targetClass     The class of the entities to be returned
     * @param setName         The name of the set to query
     * @param templateContext The context containing necessary components
     * @return A {@link Stream} of entities of the specified type
     * @throws AerospikeException if an error occurs during reading
     */
    static <T> Stream<T> find(Class<T> targetClass, String setName, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        return findRecordsUsingQuery(setName, targetClass, null, templateContext)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    /**
     * Finds records with post-processing applied based on the provided query. This method first retrieves records and
     * then applies additional processing.
     *
     * @param <T>             The type of the target class
     * @param setName         The name of the set to query
     * @param targetClass     The class of the entities to be returned
     * @param query           The query to execute, potentially containing sorting, offset, and post-processing
     *                        instructions
     * @param templateContext The context containing necessary components
     * @return A {@link Stream} of entities of the specified type after post-processing
     * @throws IllegalArgumentException if the query is unsorted combined with an offset
     */
    static <T> Stream<T> findWithPostProcessing(String setName, Class<T> targetClass, Query query,
                                                TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Stream<T> results = findUsingQueryWithDistinctPredicate(setName, targetClass, getDistinctPredicate(query),
            query, templateContext);
        return PostProcessingUtils.applyPostProcessingOnResults(results, query);
    }

    /**
     * Finds records using a query and applies a distinct predicate to filter the results. The key records are then
     * mapped to entities of the target class.
     *
     * @param <T>               The type of the target class
     * @param setName           The name of the set to query
     * @param targetClass       The class of the entities to be returned
     * @param distinctPredicate A predicate to apply for filtering distinct key records
     * @param query             The query to execute
     * @param templateContext   The context containing necessary templates and converters
     * @return A {@link Stream} of entities of the specified type after filtering by the distinct predicate
     */
    static <T> Stream<T> findUsingQueryWithDistinctPredicate(String setName, Class<T> targetClass,
                                                             Predicate<KeyRecord> distinctPredicate,
                                                             Query query, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        return findRecordsUsingQuery(setName, targetClass, query, templateContext)
            .filter(distinctPredicate)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    /**
     * Finds existing {@link KeyRecord}s using a query within a specified set. This method handles queries with ID
     * qualifiers separately as they have a different retrieval mechanism. The returned stream is closed automatically
     * when the stream is consumed or when an error occurs.
     *
     * @param setName         The name of the set to query. Must not be null
     * @param query           The query to execute
     * @param templateContext The {@link TemplateContext} containing the query engine and other necessary components
     * @return A {@link Stream} of existing {@link KeyRecord}s
     * @throws IllegalArgumentException if the set name is null
     * @throws AerospikeException       if an error occurs during the batch read
     */
    static Stream<KeyRecord> findExistingKeyRecordsUsingQuery(String setName, Query query,
                                                              TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query with id
                return BatchUtils.findByIdsWithoutEntityMapping(
                    getIdValue(idQualifier),
                    setName,
                    null,
                    new Query(excludeIdQualifier(qualifier)),
                    templateContext
                );
            }
        }
        KeyRecordIterator recIterator =
            templateContext.queryEngine.selectForCount(templateContext.namespace, setName, query);

        return StreamUtils.createStreamFromIterator(recIterator)
            .onClose(() -> {
                try {
                    recIterator.close();
                } catch (Exception e) {
                    log.error("Caught exception while closing query", e);
                }
            });
    }

    /**
     * Executes a write operation and handles any potential errors. This method enriches the provided write policy with
     * transaction details before performing the operation.
     *
     * @param data            The {@link AerospikeWriteData}, including the key and bins to write
     * @param writePolicy     The {@link WritePolicy} to use for the operation
     * @param operations      An array of {@link Operation}s to perform on the record
     * @param templateContext The {@link TemplateContext} containing the Aerospike client and exception translator
     * @return The {@link Record} after the write operation
     * @throws AerospikeException if an Aerospike-specific error occurs during the operation, translated into a more
     *                            specific exception by the {@code exceptionTranslator}
     */
    @SuppressWarnings("UnusedReturnValue")
    static Record doPersistAndHandleError(AerospikeWriteData data, WritePolicy writePolicy, Operation[] operations,
                                          TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        try {
            WritePolicy writePolicyEnriched =
                (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, writePolicy);
            return templateContext.client.operate(writePolicyEnriched, data.getKey(), operations);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    /**
     * Persists a document with version management and handles CAS errors. This method updates the document's version
     * based on the Aerospike record's generation.
     *
     * @param <T>               The type of the document
     * @param document          The document to persist
     * @param data              The {@link AerospikeWriteData}, including the key and bins to write
     * @param writePolicy       The {@link WritePolicy} to use for the operation
     * @param firstlyDeleteBins If true, existing bins will be deleted before writing new ones
     * @param operationType     The type of operation being performed (e.g., INSERT, UPDATE)
     * @param templateContext   The {@link TemplateContext} containing the Aerospike client, exception translator, and
     *                          mapping context
     * @throws AerospikeException                if an error occurs during writing
     * @throws OptimisticLockingFailureException if a CAS-related error occurs
     */
    static <T> void doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data,
                                                          WritePolicy writePolicy, boolean firstlyDeleteBins,
                                                          BaseAerospikeTemplate.OperationType operationType,
                                                          TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        try {
            Record newAeroRecord = putAndGetHeader(data, writePolicy, firstlyDeleteBins, templateContext);
            updateVersion(document, newAeroRecord, templateContext);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateCasException(e, "Failed to " + operationType.toString() + " record",
                templateContext.exceptionTranslator);
        }
    }

    /**
     * Persists a document with version management and handles errors. This method updates the document's version based
     * on the Aerospike record's generation.
     *
     * @param <T>             The type of the document
     * @param document        The document to persist
     * @param data            The {@link AerospikeWriteData}, including the key and bins to write
     * @param writePolicy     The {@link WritePolicy} to use for the operation
     * @param templateContext The {@link TemplateContext} containing the Aerospike client, exception translator, and
     *                        mapping context
     * @throws AerospikeException                if an error occurs during writing
     * @throws OptimisticLockingFailureException if a CAS-related error occurs
     */
    static <T> void doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                       TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        try {
            Record newAeroRecord = putAndGetHeader(data, writePolicy, false, templateContext);
            updateVersion(document, newAeroRecord, templateContext);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    /**
     * Performs a "put" operation and retrieves the record header. This method constructs the necessary operations and
     * enriches the write policy with transaction details.
     *
     * @param data              The {@link AerospikeWriteData}, including the key and bins to write
     * @param writePolicy       The {@link WritePolicy} to use for the operation
     * @param firstlyDeleteBins If true, existing bins will be deleted at first
     * @param templateContext   The {@link TemplateContext} containing the Aerospike client
     * @return The {@link Record} header after the put operation
     * @throws AerospikeException if client operate command fails
     */
    private static Record putAndGetHeader(AerospikeWriteData data, WritePolicy writePolicy, boolean firstlyDeleteBins,
                                          TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        Key key = data.getKey();
        Operation[] operations = getPutAndGetHeaderOperations(data, firstlyDeleteBins);
        WritePolicy writePolicyEnriched =
            (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, writePolicy);

        return templateContext.client.operate(writePolicyEnriched, key, operations);
    }

    /**
     * Finds records of a specified type within a given set and applies post-processing (sorting, offset, and limit).
     *
     * @param <T>             The type of the target class
     * @param setName         The name of the set to query
     * @param targetClass     The class of the entities to be returned
     * @param sort            The sort criteria to apply to the results
     * @param offset          The number of records to skip from the beginning of the result set
     * @param limit           The maximum number of records to return
     * @param templateContext The context containing necessary components
     * @return A {@link Stream} of entities of the specified type after post-processing
     * @throws IllegalArgumentException if there is no sorting, but an offset is provided
     * @throws AerospikeException       if an error occurs during reading
     */
    @SuppressWarnings("SameParameterValue")
    static <T> Stream<T> findWithPostProcessing(String setName, Class<T> targetClass, Sort sort, long offset,
                                                long limit, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        verifyUnsortedWithOffset(sort, offset);
        Stream<T> results = find(targetClass, setName, templateContext);
        return PostProcessingUtils.applyPostProcessingOnResults(results, sort, offset, limit);
    }

    /**
     * Finds {@link KeyRecord}s using a query within a specified set and with a specific target class. This method
     * handles queries with ID qualifiers separately because they have a different retrieval mechanism. The returned
     * stream is closed automatically when the stream is consumed or when an error occurs.
     *
     * @param <T>             The type of the target class (used for determining bin names)
     * @param setName         The name of the set to query
     * @param targetClass     The class of the entities, used to determine which bin names to retrieve
     * @param query           The query to execute
     * @param templateContext The {@link TemplateContext} containing the query engine, mapping context, and other
     *                        necessary components
     * @return A {@link Stream} of {@link KeyRecord}s that match the query
     */
    private static <T> Stream<KeyRecord> findRecordsUsingQuery(String setName, Class<T> targetClass, Query query,
                                                               TemplateContext templateContext) {
        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        String[] binNames = getBinNamesFromTargetClassOrNull(null, targetClass, templateContext.mappingContext);
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for id equality query
                return BatchUtils.findByIdsWithoutEntityMapping(
                    getIdValue(idQualifier),
                    setName,
                    binNames,
                    new Query(excludeIdQualifier(qualifier)),
                    templateContext
                );
            }
        }

        KeyRecordIterator recIterator;
        if (binNames != null) {
            recIterator = templateContext.queryEngine.select(templateContext.namespace, setName, binNames, query);
        } else {
            recIterator = templateContext.queryEngine.select(templateContext.namespace, setName, query);
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

    /**
     * Executes Aerospike operations reactively on a given value and maps the result to entity class. This method
     * performs the operations and then maps the returned {@link KeyRecord} to the entity class of the original
     * document. Error translation is applied to any {@link AerospikeException} that occurs.
     *
     * @param <T>             The type of the document
     * @param document        The original document
     * @param data            The {@link AerospikeWriteData}, including the key
     * @param writePolicy     The {@link WritePolicy} to use for the operation
     * @param operations      An array of {@link Operation}s to perform
     * @param templateContext The {@link TemplateContext} containing the reactive Aerospike client, converter, and
     *                        exception translator
     * @return A {@link Mono} that emits the mapped entity
     * @throws AerospikeException in case of an error during client operate command
     */
    static <T> Mono<T> executeOperationsReactivelyOnValue(T document, AerospikeWriteData data,
                                                          WritePolicy writePolicy, Operation[] operations,
                                                          TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        return templateContext.reactorClient.operate(writePolicy, data.getKey(), operations)
            .filter(keyRecord -> Objects.nonNull(keyRecord.record))
            .map(keyRecord ->
                MappingUtils.mapToEntity(keyRecord.key, MappingUtils.getEntityClass(document), keyRecord.record,
                    templateContext.converter))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    /**
     * Counts the number of objects in a given set across all nodes in the Aerospike cluster. This method accounts for
     * the replication factor to provide an accurate count in a replicated environment.
     *
     * @param setName         The name of the set to count
     * @param templateContext The {@link TemplateContext} containing the reactive Aerospike client and namespace
     * @return The total number of objects in the set
     */
    static long countSet(String setName, TemplateContext templateContext) {
        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        String namespace = templateContext.namespace;
        Node[] nodes = reactorClient.getAerospikeClient().getNodes();

        int replicationFactor = Utils.getReplicationFactor(reactorClient.getAerospikeClient(), nodes, namespace);

        long totalObjects = Arrays.stream(nodes)
            .mapToLong(node -> Utils.getObjectsCount(reactorClient.getAerospikeClient(), node, namespace, setName))
            .sum();

        return (nodes.length > 1) ? (totalObjects / replicationFactor) : totalObjects;
    }

    /**
     * Finds existing {@link KeyRecord}s reactively using a query within a specified set. This method handles queries
     * with ID qualifiers separately because they have a different retrieval mechanism.
     *
     * @param setName         The name of the set to query. Must not be null
     * @param query           The query to execute
     * @param templateContext The {@link TemplateContext} containing the reactive query engine
     * @return A {@link Flux} that emits the found {@link KeyRecord}s
     * @throws IllegalArgumentException if the set name is null
     * @throws AerospikeException       in case of an error during reading
     */
    static Flux<KeyRecord> findExistingKeyRecordsUsingQueryReactively(String setName, Query query,
                                                                      TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query with id
                return findByIdsWithoutEntityMappingReactively(
                    getIdValue(idQualifier),
                    setName,
                    null,
                    new Query(excludeIdQualifier(qualifier)),
                    templateContext
                );
            }
        }
        return templateContext.reactorQueryEngine.selectForCount(templateContext.namespace, setName, query);
    }

    /**
     * Persists a document reactively and handles errors. This method enriches the write policy with transaction details
     * before performing the operation.
     *
     * @param <T>             The type of the document
     * @param document        The document to persist
     * @param data            The {@link AerospikeWriteData}, including the key
     * @param writePolicy     The {@link WritePolicy} to use for the operation
     * @param operations      An array of {@link Operation}s to perform
     * @param templateContext The {@link TemplateContext} containing the reactive Aerospike client and exception
     *                        translator
     * @return A {@link Mono} that emits the original document upon successful persistence
     * @throws AerospikeException in case of an error during client operate command
     */
    static <T> Mono<T> doPersistAndHandleErrorReactively(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                         Operation[] operations, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        IAerospikeReactorClient reactorClient = templateContext.reactorClient;

        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                reactorClient.operate((WritePolicy) writePolicyEnriched, data.getKey(), operations))
            .map(docKey -> document)
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    /**
     * Persists a document reactively with version management and handles CAS errors. This method updates the document's
     * version based on the Aerospike record's generation after a successful operation.
     *
     * @param <T>             The type of the document
     * @param document        The document to persist
     * @param data            The {@link AerospikeWriteData}, including the key
     * @param writePolicy     The {@link WritePolicy} to use for the operation
     * @param operations      An array of {@link Operation}s to perform
     * @param operationType   The type of operation being performed (e.g., INSERT, UPDATE)
     * @param templateContext The {@link TemplateContext} containing the reactive Aerospike client, exception
     *                        translator, and mapping context
     * @return A {@link Mono} that emits the updated document upon successful persistence
     * @throws AerospikeException                if an error occurs during writing
     * @throws OptimisticLockingFailureException if a CAS-related error occurs
     */
    static <T> Mono<T> doPersistWithVersionAndHandleCasErrorReactively(T document, AerospikeWriteData data,
                                                                       WritePolicy writePolicy, Operation[] operations,
                                                                       BaseAerospikeTemplate.OperationType operationType,
                                                                       TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        return PolicyUtils.enrichPolicyWithTransaction(templateContext.reactorClient, writePolicy)
            .flatMap(writePolicyEnriched -> putAndGetHeaderForReactive(data, (WritePolicy) writePolicyEnriched,
                operations, templateContext))
            .map(newRecord -> updateVersion(document, newRecord, templateContext))
            .onErrorMap(AerospikeException.class, i -> ExceptionUtils.translateCasException(i,
                "Failed to " + operationType.toString() + " record due to versions mismatch",
                templateContext.exceptionTranslator));
    }

    /**
     * Persists a document reactively with version management and handles general errors. This method updates the
     * document's version based on the Aerospike record's generation after a successful operation.
     *
     * @param <T>             The type of the document
     * @param document        The document to persist
     * @param data            The {@link AerospikeWriteData}, including the key
     * @param writePolicy     The {@link WritePolicy} to use for the operation
     * @param operations      An array of {@link Operation}s to perform
     * @param templateContext The {@link TemplateContext} containing the reactive Aerospike client, exception
     *                        translator, and mapping context
     * @return A {@link Mono} that emits the updated document upon successful persistence
     * @throws AerospikeException                if an error occurs during writing
     * @throws OptimisticLockingFailureException if a CAS-related error occurs
     */
    static <T> Mono<T> doPersistWithVersionAndHandleErrorReactively(T document, AerospikeWriteData data,
                                                                    WritePolicy writePolicy, Operation[] operations,
                                                                    TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        return PolicyUtils.enrichPolicyWithTransaction(templateContext.reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                putAndGetHeaderForReactive(data, (WritePolicy) writePolicyEnriched, operations, templateContext))
            .map(newRecord -> updateVersion(document, newRecord, templateContext))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    /**
     * Performs a "put" operation reactively and retrieves the record header.
     *
     * @param data            The {@link AerospikeWriteData}, including the key
     * @param writePolicy     The {@link WritePolicy} to use for the operation
     * @param operations      An array of {@link Operation}s to perform
     * @param templateContext The {@link TemplateContext} containing the reactive Aerospike client
     * @return A {@link Mono} that emits the {@link Record} header after the put operation
     */
    private static Mono<Record> putAndGetHeaderForReactive(AerospikeWriteData data, WritePolicy writePolicy,
                                                           Operation[] operations, TemplateContext templateContext) {
        return templateContext.reactorClient.operate(writePolicy, data.getKey(), operations)
            .map(keyRecord -> keyRecord.record);
    }

    /**
     * Touches a record (updates its expiration time) and retrieves its contents reactively. The expiration time is set
     * based on the provided {@code expiration} value. If {@code binNames} are provided, only those bins will be
     * retrieved; otherwise, all bins will be returned. A filter expression can be applied based on the criteria in the
     * {@code query}.
     *
     * @param key             The {@link Key} of the record to touch and get
     * @param expiration      The new expiration time for the record in seconds from now
     * @param binNames        An optional array of bin names to retrieve. If null or empty, all bins will be retrieved
     * @param query           An optional {@link Query} object containing criteria for filtering
     * @param templateContext The context containing the reactive Aerospike client and query engine
     * @return A {@link Mono} that emits the {@link KeyRecord} after the touch and get operation
     * @throws AerospikeException in case of an error during client operate command
     */
    static Mono<KeyRecord> touchAndGetReactively(Key key, int expiration, String[] binNames, Query query,
                                                 TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(templateContext.writePolicyDefault)
            .expiration(expiration);

        if (isQueryCriteriaNotNull(query)) {
            Qualifier qualifier = query.getCriteriaObject();
            writePolicyBuilder.filterExp(templateContext.reactorQueryEngine.getFilterExpressionsBuilder()
                .build(qualifier));
        }
        WritePolicy writePolicy = writePolicyBuilder.build();

        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        if (binNames == null || binNames.length == 0) {
            return PolicyUtils.enrichPolicyWithTransaction(reactorClient, writePolicy)
                .flatMap(writePolicyEnriched ->
                    reactorClient.operate((WritePolicy) writePolicyEnriched, key, Operation.touch(), Operation.get()));
        }
        Operation[] operations = new Operation[binNames.length + 1];
        operations[0] = Operation.touch();

        for (int i = 1; i < operations.length; i++) {
            operations[i] = Operation.get(binNames[i - 1]);
        }
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched -> reactorClient.operate((WritePolicy) writePolicyEnriched, key, operations));
    }

    /**
     * Finds records of a specified type within a given set reactively and applies post-processing based on the provided
     * query. This method first retrieves records using a distinct predicate and then applies additional processing.
     *
     * @param <T>             The type of the target class
     * @param setName         The name of the set to query
     * @param targetClass     The class of the entities to be returned
     * @param query           The {@link Query} to execute, potentially containing post-processing instructions
     * @param templateContext The {@link TemplateContext} containing necessary components
     * @return A {@link Flux} of entities of the specified type after post-processing
     * @throws IllegalArgumentException if the query contains an unsorted sort combined with an offset
     * @throws AerospikeException       in case of an error during reading
     */
    static <T> Flux<T> findWithPostProcessingReactively(String setName, Class<T> targetClass, Query query,
                                                        TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Flux<T> results = findUsingQueryWithDistinctPredicateReactively(setName, targetClass,
            getDistinctPredicate(query), query, templateContext);
        results = PostProcessingUtils.applyPostProcessingOnResults(results, query);
        return results;
    }

    /**
     * Finds records of a specified type within a given set reactively and applies post-processing including sorting,
     * offset, and limit.
     *
     * @param <T>             The type of the target class
     * @param setName         The name of the set to query
     * @param targetClass     The class of the entities to be returned
     * @param sort            The sort criteria to apply to the results
     * @param offset          The number of records to skip from the beginning of the result set
     * @param limit           The maximum number of records to return
     * @param templateContext The {@link TemplateContext} containing necessary components
     * @return A {@link Flux} of entities of the specified type after post-processing
     * @throws IllegalArgumentException if the sort criteria is unsorted and an offset is provided
     * @throws AerospikeException       if there is an error during reading
     */
    @SuppressWarnings("SameParameterValue")
    static <T> Flux<T> findWithPostProcessingReactively(String setName, Class<T> targetClass, Sort sort, long offset,
                                                        long limit, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        verifyUnsortedWithOffset(sort, offset);
        Flux<T> results = findReactively(setName, targetClass, templateContext);
        results = PostProcessingUtils.applyPostProcessingOnResults(results, sort, offset, limit);
        return results;
    }

    /**
     * Finds records of a specified type within a given set reactively. This method maps the retrieved key records to
     * the target class.
     *
     * @param <T>             The type of the target class
     * @param setName         The name of the set to query
     * @param targetClass     The class of the entities to be returned
     * @param templateContext The {@link TemplateContext} containing necessary components
     * @return A {@link Flux} of entities of the specified type
     * @throws AerospikeException if there is an error during reading
     */
    static <T> Flux<T> findReactively(String setName, Class<T> targetClass, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        return findRecordsUsingQueryReactively(setName, targetClass, null, templateContext)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    /**
     * Finds records reactively using a query and applies a distinct predicate to filter the results. The key records
     * are then mapped to the target class.
     *
     * @param <T>               The type of the target class
     * @param setName           The name of the set to query
     * @param targetClass       The class of the entities to be returned
     * @param distinctPredicate A predicate to apply for filtering distinct key records
     * @param query             The {@link Query} to execute
     * @param templateContext   The {@link TemplateContext} containing necessary components
     * @return A {@link Flux} of entities of the specified type after filtering by the distinct predicate
     * @throws AerospikeException if there is an error during reading
     */
    static <T> Flux<T> findUsingQueryWithDistinctPredicateReactively(String setName, Class<T> targetClass,
                                                                     Predicate<KeyRecord> distinctPredicate,
                                                                     Query query, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        return findRecordsUsingQueryReactively(setName, targetClass, query, templateContext)
            .filter(distinctPredicate)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    /**
     * Finds {@link KeyRecord}s reactively using a query within a specified set and with a specific target class. This
     * method handles queries with ID qualifiers separately because they have a different retrieval mechanism. It can
     * also specify which bin names to retrieve if a {@code targetClass} is provided.
     *
     * @param <T>             The type of the target class (used for determining bin names)
     * @param setName         The name of the set to query
     * @param targetClass     The class of the entities, used to determine which bin names to retrieve. If {@code null},
     *                        all bins are retrieved
     * @param query           The {@link Query} to execute
     * @param templateContext The {@link TemplateContext} containing the reactive query engine, mapping context, and
     *                        other necessary components
     * @return A {@link Flux} of {@link KeyRecord}s that match the query
     * @throws AerospikeException if there is an error during reading
     */
    private static <T> Flux<KeyRecord> findRecordsUsingQueryReactively(String setName, Class<T> targetClass,
                                                                       Query query,
                                                                       TemplateContext templateContext) {
        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for id equality query
                return findByIdsWithoutEntityMappingReactively(
                    getIdValue(idQualifier),
                    setName,
                    targetClass,
                    new Query(excludeIdQualifier(qualifier)),
                    templateContext
                );
            }
        }

        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass, templateContext.mappingContext);
            return templateContext.reactorQueryEngine.select(templateContext.namespace, setName, binNames, query);
        }
        return templateContext.reactorQueryEngine.select(templateContext.namespace, setName, null, query);
    }

    /**
     * Finds existing {@link KeyRecord}s reactively by a collection of IDs without mapping the results to entities. This
     * method uses a batch policy that can include filter expressions from the provided query. It asserts that both
     * {@code ids} and {@code setName} are not null and returns an empty Flux if {@code ids} is empty.
     *
     * @param <T>             The type of the target class (not used for mapping, but for type consistency in other
     *                        methods)
     * @param ids             A collection of IDs to query for. Must not be null
     * @param setName         The name of the set to query. Must not be null
     * @param targetClass     The class of the entities (used for internal purposes like bin name retrieval in some
     *                        flows, can be null)
     * @param query           The {@link Query} object, potentially containing criteria for batch policy filtering
     * @param templateContext The {@link TemplateContext} containing the reactive Aerospike client and other necessary
     *                        components
     * @return A {@link Flux} of existing {@link KeyRecord}s
     * @throws IllegalArgumentException if {@code ids} or {@code setName} is null
     * @throws AerospikeException       in case of an error during reading
     */
    private static <T> Flux<KeyRecord> findByIdsWithoutEntityMappingReactively(Collection<?> ids, String setName,
                                                                               Class<T> targetClass,
                                                                               Query query,
                                                                               TemplateContext templateContext) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        if (ids.isEmpty()) {
            return Flux.empty();
        }

        BatchPolicy batchPolicy = BatchUtils.getBatchPolicyForReactive(query, templateContext);
        Key[] keys = getKeys(iterableToList(ids), setName, templateContext).toArray(Key[]::new);

        return BatchUtils.batchReadInChunksReactively(batchPolicy, keys, targetClass, templateContext)
            .filter(keyRecord -> keyRecord != null && keyRecord.record != null);
    }

    /**
     * Returns a {@link Comparator} that can be used to sort entities based on the sort orders defined in a
     * {@link Query}. The comparator is built by chaining comparators for each sort order.
     *
     * @param <T>   The type of the entities to compare
     * @param query The {@link Query} containing the sort orders
     * @return A {@link Comparator} for the specified type
     * @throws IllegalStateException if the query's sort orders are empty, as comparator cannot be created
     */
    static <T> Comparator<T> getComparator(Query query) {
        return query.getSort().stream()
            .map(TemplateUtils::<T>getPropertyComparator)
            .reduce(Comparator::thenComparing)
            .orElseThrow(() -> new IllegalStateException("Comparator can not be created if sort orders are empty"));
    }

    /**
     * Returns a {@link Comparator} that can be used to sort entities based on the provided {@link Sort} object. The
     * comparator is built by chaining comparators for each sort order.
     *
     * @param <T>  The type of the entities to compare
     * @param sort The {@link Sort} object containing the sort orders
     * @return A {@link Comparator} for the specified type
     * @throws IllegalStateException if the sort orders are empty, as comparator cannot be created
     */
    static <T> Comparator<T> getComparator(Sort sort) {
        return sort.stream()
            .map(TemplateUtils::<T>getPropertyComparator)
            .reduce(Comparator::thenComparing)
            .orElseThrow(() -> new IllegalStateException("Comparator can not be created if sort orders are empty"));
    }

    /**
     * Creates a {@link Comparator} for a specific property of an entity based on the provided {@link Sort.Order}. This
     * comparator supports case-insensitive comparison and can sort in ascending or descending order.
     *
     * @param <T>   The type of the entities to compare
     * @param order The {@link Sort.Order} specifying the property and direction
     * @return A {@link Comparator} for the specified property
     */
    private static <T> Comparator<T> getPropertyComparator(Sort.Order order) {
        boolean ignoreCase = true;
        boolean ascending = order.getDirection().isAscending();
        return new PropertyComparator<>(order.getProperty(), ignoreCase, ascending);
    }

    /**
     * Creates an {@link AerospikeWriteData} object for a given document and set name. This method converts the document
     * into Aerospike bins using the configured converter.
     *
     * @param <T>             The type of the document
     * @param document        The document to write
     * @param setName         The name of the Aerospike set
     * @param templateContext The {@link TemplateContext} containing the namespace and converter
     * @return An {@link AerospikeWriteData} object containing the key, set name, and bins
     */
    static <T> AerospikeWriteData writeData(T document, String setName, TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        AerospikeWriteData data = AerospikeWriteData.forWrite(templateContext.namespace);
        data.setSetName(setName);
        templateContext.converter.write(document, data);
        return data;
    }

    /**
     * Creates an {@link AerospikeWriteData} object for a given document, set name, and a specific collection of fields.
     * This method converts only the specified fields of the document into Aerospike bins.
     *
     * @param <T>             The type of the document
     * @param document        The document to write
     * @param setName         The name of the Aerospike set
     * @param fields          A collection of field names to include in the write operation
     * @param templateContext The {@link TemplateContext} containing the namespace, converter, and other necessary
     *                        components
     * @return An {@link AerospikeWriteData} object containing the key, set name, and bins for the specified fields
     */
    static <T> AerospikeWriteData writeDataWithSpecificFields(T document, String setName, Collection<String> fields,
                                                              TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        AerospikeWriteData data = AerospikeWriteData.forWrite(templateContext.namespace);
        data.setSetName(setName);
        data.setRequestedBins(MappingUtils.fieldsToBinNames(document, fields, templateContext));
        templateContext.converter.write(document, data);
        return data;
    }

    /**
     * Creates an Aerospike {@link Key} from an ID and an {@link AerospikePersistentEntity}. This method delegates to
     * {@link #getKey(Object, String, TemplateContext)} using the set name from the entity.
     *
     * @param id              The ID of the record. Must not be null
     * @param entity          The {@link AerospikePersistentEntity} from which to derive the set name
     * @param templateContext The {@link TemplateContext} containing the converter
     * @return An Aerospike {@link Key}
     */
    static Key getKey(Object id, AerospikePersistentEntity<?> entity, TemplateContext templateContext) {
        return getKey(id, entity.getSetName(), templateContext);
    }

    /**
     * Creates an Aerospike {@link Key} from an ID and a set name. The ID type is preserved based on the configuration
     * of the Aerospike data settings within the converter. If {@code keepOriginalKeyTypes} is false, the ID is
     * converted to a String.
     *
     * @param id              The ID of the record. Must not be null
     * @param setName         The name of the Aerospike set. Must not be null
     * @param templateContext The {@link TemplateContext} containing the namespace and converter
     * @return An Aerospike {@link Key}
     * @throws IllegalArgumentException if {@code id} or {@code setName} is null
     */
    static Key getKey(Object id, String setName, TemplateContext templateContext) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Assert.notNull(templateContext, "TemplateContext name must not be null!");

        // Choosing whether to preserve id type based on the configuration
        if (templateContext.converter.getAerospikeDataSettings().isKeepOriginalKeyTypes()) {
            if (id instanceof Byte || id instanceof Short || id instanceof Integer || id instanceof Long) {
                return new Key(
                    templateContext.namespace,
                    setName,
                    convertIfNecessary(templateContext.converter, ((Number) id).longValue(), Long.class)
                );
            } else if (id instanceof Character) {
                return new Key(
                    templateContext.namespace,
                    setName,
                    convertIfNecessary(templateContext.converter, id, Character.class)
                );
            } else if (id instanceof byte[]) {
                return new Key(
                    templateContext.namespace,
                    setName,
                    convertIfNecessary(templateContext.converter, id, byte[].class)
                );
            }
        }

        return new Key(
            templateContext.namespace,
            setName,
            convertIfNecessary(templateContext.converter, id, String.class)
        );
    }

    /**
     * Creates an array of Aerospike client {@link Operation}s for a "put" operation followed by a "get header"
     * operation. Optionally includes a "delete" operation for existing bins if {@code firstlyDeleteBins} is true.
     *
     * @param data              The {@link AerospikeWriteData} containing the bins to put
     * @param firstlyDeleteBins If true, an {@code Operation.delete()} will be included before the put operation
     * @return An array of {@link Operation}s
     * @throws AerospikeException if the document has no bins and the class bin is disabled, as a put operation cannot
     *                            be performed
     */
    static Operation[] getPutAndGetHeaderOperations(AerospikeWriteData data, boolean firstlyDeleteBins) {
        Bin[] bins = data.getBinsAsArray();

        if (bins.length == 0) {
            throw new AerospikeException(
                "Cannot put and get header on a document with no bins and class bin disabled.");
        }

        return operations(bins, Operation::put, firstlyDeleteBins ? Operation.array(Operation.delete()) : null,
            Operation.array(Operation.getHeader()));
    }

    /**
     * Creates an array of Aerospike client {@link Operation}s from a map of bin names to values. Each entry in the map
     * is converted into an operation of the specified {@code operationType}. Additional operations can be appended to
     * the array.
     *
     * @param <T>                  The type of the values in the map
     * @param values               A map where keys are bin names and values are the corresponding data
     * @param operationType        The type of Aerospike operation to apply to each bin (e.g., PUT, ADD)
     * @param additionalOperations An array of additional {@link Operation}s to append
     * @return An array of {@link Operation}s
     */
    static <T> Operation[] operations(Map<String, T> values,
                                      Operation.Type operationType,
                                      Operation... additionalOperations) {
        Operation[] operations = new Operation[values.size() + additionalOperations.length];
        int x = 0;
        for (Map.Entry<String, T> entry : values.entrySet()) {
            operations[x] = new Operation(operationType, entry.getKey(), Value.get(entry.getValue()));
            x++;
        }
        for (Operation additionalOp : additionalOperations) {
            operations[x] = additionalOp;
            x++;
        }
        return operations;
    }

    /**
     * Creates an array of Aerospike client {@link Operation}s from an array of {@link Bin}s using a provided function.
     * Each {@link Bin} is transformed into an {@link Operation} by the {@code binToOperation} function.
     *
     * @param bins           An array of {@link Bin} objects
     * @param binToOperation A function that converts a {@link Bin} into an {@link Operation}
     * @return An array of {@link Operation}s
     */
    static Operation[] operations(Bin[] bins, Function<Bin, Operation> binToOperation) {
        return operations(bins, binToOperation, null, null);
    }

    /**
     * Creates an array of Aerospike client {@link Operation}s from an array of {@link Bin}s, including operations that
     * precede the bin operations.
     *
     * @param bins                An array of {@link Bin} objects
     * @param binToOperation      A function that converts a {@link Bin} into an {@link Operation}
     * @param precedingOperations An array of {@link Operation}s to place before the bin operations. Can be null
     * @return An array of {@link Operation}s
     */
    static Operation[] operations(Bin[] bins, Function<Bin, Operation> binToOperation,
                                  Operation[] precedingOperations) {
        return operations(bins, binToOperation, precedingOperations, null);
    }

    /**
     * Creates an array of Aerospike client {@link Operation}s from an array of {@link Bin}s, including both preceding
     * and additional operations. Each {@link Bin} is transformed into an {@link Operation} by the
     * {@code binToOperation} function.
     *
     * @param bins                 An array of {@link Bin} objects
     * @param binToOperation       A function that converts a {@link Bin} into an {@link Operation}
     * @param precedingOperations  An array of {@link Operation}s to place before the bin operations. Can be null
     * @param additionalOperations An array of additional {@link Operation}s to place after the bin operations. Can be
     *                             null
     * @return An array of {@link Operation}s
     */
    static Operation[] operations(Bin[] bins,
                                  Function<Bin, Operation> binToOperation,
                                  @Nullable Operation[] precedingOperations,
                                  @Nullable Operation[] additionalOperations) {
        int precedingOpsLength = precedingOperations == null ? 0 : precedingOperations.length;
        int additionalOpsLength = additionalOperations == null ? 0 : additionalOperations.length;
        Operation[] operations = new Operation[precedingOpsLength + bins.length + additionalOpsLength];
        int i = 0;
        if (precedingOpsLength > 0) {
            for (Operation precedingOp : precedingOperations) {
                operations[i] = precedingOp;
                i++;
            }
        }
        for (Bin bin : bins) {
            operations[i] = binToOperation.apply(bin);
            i++;
        }
        if (additionalOpsLength > 0) {
            for (Operation additionalOp : additionalOperations) {
                operations[i] = additionalOp;
                i++;
            }
        }
        return operations;
    }

    /**
     * Returns a {@link Predicate} for filtering distinct {@link KeyRecord}s based on a {@link Query}. If the query
     * specifies a distinct operation, the predicate ensures that only records with unique values in the specified
     * distinct field are passed. Currently, distinct queries are only supported for top-level object fields (not
     * dot-path fields).
     *
     * @param query The {@link Query} to inspect for distinct criteria
     * @return A {@link Predicate} that filters for distinct {@link KeyRecord}s
     * @throws UnsupportedOperationException if a distinct query is attempted on a dot-path field
     */
    static Predicate<KeyRecord> getDistinctPredicate(Query query) {
        Predicate<KeyRecord> distinctPredicate;
        if (query != null && query.isDistinct()) {
            List<String> dotPathList = query.getCriteriaObject().getDotPath();
            if (dotPathList != null && !dotPathList.isEmpty() && dotPathList.get(0) != null) {
                String dotPathString = String.join(",", query.getCriteriaObject().getDotPath());
                throw new UnsupportedOperationException("DISTINCT queries are currently supported only for the first " +
                    "level objects, got a query for " + dotPathString);
            }

            final Set<Object> distinctValues = ConcurrentHashMap.newKeySet();
            distinctPredicate = kr -> {
                final String distinctField = query.getCriteriaObject().getBinName();
                if (kr.record == null || kr.record.bins == null) {
                    return false;
                }
                return distinctValues.add(kr.record.bins.get(distinctField));
            };
        } else {
            distinctPredicate = kr -> true;
        }

        return distinctPredicate;
    }

    /**
     * Finds entities by their IDs using a query and applies post-processing to the results.
     *
     * <p>This method retrieves a stream of entities based on an Iterable of IDs, an entity class, and a target class.
     * It utilizes a provided {@link Query} for filtering and then applies post-processing to the retrieved entities
     * using {@link PostProcessingUtils#applyPostProcessingOnResults}.</p>
     *
     * @param <T>             The type of the entity
     * @param <S>             The type of the target class to which the entities will be mapped
     * @param ids             An {@link Iterable} of IDs of the entities to find
     * @param entityClass     The {@link Class} of the entity
     * @param targetClass     The {@link Class} to which the retrieved entities will be mapped, can be {@code null}
     * @param setName         The name of the set where the entities are stored
     * @param query           A {@link Query} to apply initial filtering or criteria to the search, can be {@code null}
     * @param templateContext The template context to be used
     * @return A {@link Stream} of entities, mapped to the target class, after post-processing
     */
    static <T, S> Stream<?> findByIdsUsingQueryWithPostProcessing(Iterable<?> ids, Class<T> entityClass,
                                                                  @Nullable Class<S> targetClass, String setName,
                                                                  @Nullable Query query,
                                                                  TemplateContext templateContext) {
        Assert.notNull(templateContext, "TemplateContext name must not be null!");
        Stream<?> results = BatchUtils.findByIdsWithoutPostProcessing(ids, entityClass, targetClass, setName, query,
            templateContext);
        return PostProcessingUtils.applyPostProcessingOnResults(results, query);
    }

    static boolean queryHasServerVersionSupport(Query query) {
        return query.getCriteria() != null && query.getCriteriaObject() != null
            && !query.getCriteriaObject().hasServerVersionSupport();
    }
}
