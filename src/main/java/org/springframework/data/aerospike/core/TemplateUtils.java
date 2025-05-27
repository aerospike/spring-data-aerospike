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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;
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

    static <T> ConvertingPropertyAccessor<T> getPropertyAccessor(AerospikePersistentEntity<?> entity, T source,
                                                                 MappingAerospikeConverter converter) {
        PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
        return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
    }

    static <T> T updateVersion(T document, Record newAeroRecord,
                               TemplateContext templateParameters) {
        AerospikePersistentEntity<?> entity =
            templateParameters.mappingContext.getRequiredPersistentEntity(document.getClass());
        ConvertingPropertyAccessor<T> propertyAccessor =
            getPropertyAccessor(entity, document, templateParameters.converter);
        AerospikePersistentProperty versionProperty = entity.getRequiredVersionProperty();
        propertyAccessor.setProperty(versionProperty, newAeroRecord.generation);
        return document;
    }

    static void logEmptyItems(Logger log, String iterableDescription) {
        log.debug("{} are empty", iterableDescription);
    }

    static boolean doDeleteWithVersionAndHandleCasError(AerospikeWriteData data,
                                                        TemplateContext templateContext) {
        try {
            WritePolicy writePolicy =
                PolicyUtils.expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY, templateContext.writePolicyDefault);
            //noinspection DataFlowIssue
            writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, writePolicy);
            return templateContext.client.delete(writePolicy, data.getKey());
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateCasError(e, "Failed to delete record due to versions mismatch",
                templateContext.exceptionTranslator);
        }
    }

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

    static Record getRecord(AerospikePersistentEntity<?> entity, Key key, @Nullable Query query,
                            TemplateContext templateContext) {
        Record aeroRecord;
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), templateContext, null, null);
        } else {
            Policy policy = PolicyUtils.enrichPolicyWithTransaction(
                templateContext.client,
                PolicyUtils.getPolicyFilterExpOrDefault(templateContext.client, templateContext.queryEngine, query));
            aeroRecord = templateContext.client.get(policy, key);
        }
        return aeroRecord;
    }

    static <S> Object getRecordMapToTargetClass(AerospikePersistentEntity<?> entity, Key key, Class<S> targetClass,
                                                @Nullable Query query, TemplateContext templateContext) {
        Record aeroRecord;
        String[] binNames = getBinNamesFromTargetClass(targetClass, templateContext.mappingContext);
        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
            aeroRecord = getAndTouch(key, entity.getExpiration(), templateContext, binNames, query);
        } else {
            Policy policy = PolicyUtils.enrichPolicyWithTransaction(
                templateContext.client,
                PolicyUtils.getPolicyFilterExpOrDefault(templateContext.client, templateContext.queryEngine, query)
            );
            aeroRecord = templateContext.client.get(policy, key, binNames);
        }
        return MappingUtils.mapToEntity(key, targetClass, aeroRecord, templateContext.converter);
    }

    static Record getAndTouch(Key key, int expiration, TemplateContext templateContext, String[] binNames, @Nullable Query query) {
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

    static <T> Stream<T> find(Class<T> targetClass, String setName, TemplateContext templateContext) {
        return findRecordsUsingQuery(setName, targetClass, null, templateContext)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    static <T> Stream<T> findWithPostProcessing(String setName, Class<T> targetClass, Query query,
                                                 TemplateContext templateContext) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Stream<T> results = findUsingQueryWithDistinctPredicate(setName, targetClass, getDistinctPredicate(query),
            query, templateContext);
        return PostProcessingUtils.applyPostProcessingOnResults(results, query);
    }

    static <T> Stream<T> findUsingQueryWithDistinctPredicate(String setName, Class<T> targetClass,
                                                                     Predicate<KeyRecord> distinctPredicate,
                                                                     Query query, TemplateContext templateContext) {
        return findRecordsUsingQuery(setName, targetClass, query, templateContext)
            .filter(distinctPredicate)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    static Stream<KeyRecord> findExistingKeyRecordsUsingQuery(String setName, Query query,
                                                              TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");

        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query with id
                return BatchUtils.findExistingByIdsWithoutMapping(
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

    static Record doPersistAndHandleError(AerospikeWriteData data, WritePolicy writePolicy, Operation[] operations,
                                          TemplateContext templateContext) {
        try {
            WritePolicy writePolicyEnriched =
                (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, writePolicy);
            return templateContext.client.operate(writePolicyEnriched, data.getKey(), operations);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    static <T> void doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data,
                                                                  WritePolicy writePolicy, boolean firstlyDeleteBins,
                                                                  BaseAerospikeTemplate.OperationType operationType,
                                                                  TemplateContext templateContext) {
        try {
            Record newAeroRecord = putAndGetHeader(data, writePolicy, firstlyDeleteBins, templateContext);
            updateVersion(document, newAeroRecord, templateContext);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateCasError(e, "Failed to " + operationType.toString() + " record due to versions mismatch",
                templateContext.exceptionTranslator);
        }
    }

    static <T> void doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                        TemplateContext templateContext) {
        try {
            Record newAeroRecord = putAndGetHeader(data, writePolicy, false, templateContext);
            updateVersion(document, newAeroRecord, templateContext);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    static Record putAndGetHeader(AerospikeWriteData data, WritePolicy writePolicy, boolean firstlyDeleteBins,
                                          TemplateContext templateContext) {
        Key key = data.getKey();
        Operation[] operations = getPutAndGetHeaderOperations(data, firstlyDeleteBins);
        WritePolicy writePolicyEnriched =
            (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(templateContext.client, writePolicy);

        return templateContext.client.operate(writePolicyEnriched, key, operations);
    }

    @SuppressWarnings("SameParameterValue")
    static <T> Stream<T> findWithPostProcessing(String setName, Class<T> targetClass, Sort sort, long offset,
                                                long limit, TemplateContext templateContext) {
        verifyUnsortedWithOffset(sort, offset);
        Stream<T> results = find(targetClass, setName, templateContext);
        return PostProcessingUtils.applyPostProcessingOnResults(results, sort, offset, limit);
    }

    static <T> Stream<KeyRecord> findRecordsUsingQuery(String setName, Class<T> targetClass, Query query,
                                                        TemplateContext templateContext) {
        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query for id equality
                return BatchUtils.findExistingByIdsWithoutMapping(
                    getIdValue(idQualifier),
                    setName,
                    targetClass,
                    new Query(excludeIdQualifier(qualifier)),
                    templateContext
                );
            }
        }

        KeyRecordIterator recIterator;
        if (targetClass != null) {
            String[] binNames = getBinNamesFromTargetClass(targetClass, templateContext.mappingContext);
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

    static <T> Mono<T> executeOperationsReactivelyOnValue(T document, AerospikeWriteData data,
                                                          WritePolicy writePolicy, Operation[] operations,
                                                          TemplateContext templateContext) {
        return templateContext.reactorClient.operate(writePolicy, data.getKey(), operations)
            .filter(keyRecord -> Objects.nonNull(keyRecord.record))
            .map(keyRecord ->
                MappingUtils.mapToEntity(keyRecord.key, MappingUtils.getEntityClass(document), keyRecord.record, templateContext.converter))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    static <T> Flux<T> findByIdsReactively(Collection<?> ids, Class<T> targetClass, String setName,
                                           TemplateContext templateContext) {
        Key[] keys = iterableToList(ids).stream()
            .map(id -> getKey(id, setName, templateContext))
            .toArray(Key[]::new);

        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient().copyBatchPolicyDefault())
            .flatMap(batchPolicy -> reactorClient.get((BatchPolicy) batchPolicy, keys))
            .flatMap(kr -> Mono.just(kr.asMap()))
            .flatMapMany(keyRecordMap -> {
                List<T> entities = keyRecordMap.entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .map(entry -> MappingUtils.mapToEntity(entry.getKey(), targetClass, entry.getValue(), templateContext.converter))
                    .collect(Collectors.toList());
                return Flux.fromIterable(entities);
            });
    }

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

    static Flux<KeyRecord> findExistingKeyRecordsUsingQueryReactively(String setName, Query query,
                                                                      TemplateContext templateContext) {
        Assert.notNull(setName, "Set name must not be null!");

        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query with id
                return findExistingByIdsWithoutMappingReactively(
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

    static <T> Mono<T> doPersistAndHandleErrorReactively(T document, AerospikeWriteData data, WritePolicy writePolicy,
                                                         Operation[] operations, TemplateContext templateContext) {
        IAerospikeReactorClient reactorClient = templateContext.reactorClient;
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                reactorClient.operate((WritePolicy) writePolicyEnriched, data.getKey(), operations))
            .map(docKey -> document)
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    static <T> Mono<T> doPersistWithVersionAndHandleCasErrorReactively(T document, AerospikeWriteData data,
                                                                       WritePolicy writePolicy, Operation[] operations,
                                                                       BaseAerospikeTemplate.OperationType operationType,
                                                                       TemplateContext templateContext) {
        return PolicyUtils.enrichPolicyWithTransaction(templateContext.reactorClient, writePolicy)
            .flatMap(writePolicyEnriched -> putAndGetHeaderForReactive(data, (WritePolicy) writePolicyEnriched, operations, templateContext))
            .map(newRecord -> updateVersion(document, newRecord, templateContext))
            .onErrorMap(AerospikeException.class, i -> ExceptionUtils.translateCasError(i,
                "Failed to " + operationType.toString() + " record due to versions mismatch",
                templateContext.exceptionTranslator));
    }

    static <T> Mono<T> doPersistWithVersionAndHandleErrorReactively(T document, AerospikeWriteData data,
                                                                    WritePolicy writePolicy, Operation[] operations,
                                                                    TemplateContext templateContext) {
        return PolicyUtils.enrichPolicyWithTransaction(templateContext.reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                putAndGetHeaderForReactive(data, (WritePolicy) writePolicyEnriched, operations, templateContext))
            .map(newRecord -> updateVersion(document, newRecord, templateContext))
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    static Mono<Record> putAndGetHeaderForReactive(AerospikeWriteData data, WritePolicy writePolicy, Operation[] operations,
                                                   TemplateContext templateContext) {
        return templateContext.reactorClient.operate(writePolicy, data.getKey(), operations)
            .map(keyRecord -> keyRecord.record);
    }

    static Mono<KeyRecord> getAndTouchReactively(Key key, int expiration, String[] binNames, Query query,
                                                 TemplateContext templateContext) {
        WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(templateContext.writePolicyDefault)
            .expiration(expiration);

        if (isQueryCriteriaNotNull(query)) {
            Qualifier qualifier = query.getCriteriaObject();
            writePolicyBuilder.filterExp(templateContext.reactorQueryEngine.getFilterExpressionsBuilder().build(qualifier));
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

    static <T> Flux<T> findWithPostProcessingReactively(String setName, Class<T> targetClass, Query query,
                                                        TemplateContext templateContext) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        Flux<T> results = findUsingQueryWithDistinctPredicateReactively(setName, targetClass,
            getDistinctPredicate(query), query, templateContext);
        results = PostProcessingUtils.applyPostProcessingOnResults(results, query);
        return results;
    }

    @SuppressWarnings("SameParameterValue")
    static <T> Flux<T> findWithPostProcessingReactively(String setName, Class<T> targetClass, Sort sort, long offset,
                                                        long limit, TemplateContext templateContext) {
        verifyUnsortedWithOffset(sort, offset);
        Flux<T> results = findReactively(setName, targetClass, templateContext);
        results = PostProcessingUtils.applyPostProcessingOnResults(results, sort, offset, limit);
        return results;
    }

    static <T> Flux<T> findReactively(String setName, Class<T> targetClass, TemplateContext templateContext) {
        return findRecordsUsingQueryReactively(setName, targetClass, null, templateContext)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    static <T> Flux<T> findUsingQueryWithDistinctPredicateReactively(String setName, Class<T> targetClass,
                                                                     Predicate<KeyRecord> distinctPredicate,
                                                                     Query query, TemplateContext templateContext) {
        return findRecordsUsingQueryReactively(setName, targetClass, query, templateContext)
            .filter(distinctPredicate)
            .map(keyRecord -> MappingUtils.mapToEntity(keyRecord, targetClass, templateContext.converter));
    }

    static <T> Flux<KeyRecord> findRecordsUsingQueryReactively(String setName, Class<T> targetClass, Query query,
                                                               TemplateContext templateContext) {
        Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
        if (qualifier != null) {
            Qualifier idQualifier = getIdQualifier(qualifier);
            if (idQualifier != null) {
                // a separate flow for a query for id equality
                return findExistingByIdsWithoutMappingReactively(
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

    // Without mapping results to entities
    static <T> Flux<KeyRecord> findExistingByIdsWithoutMappingReactively(Collection<?> ids, String setName,
                                                                         Class<T> targetClass, Query query,
                                                                         TemplateContext templateContext) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        if (ids.isEmpty()) {
            return Flux.empty();
        }

        BatchPolicy batchPolicy = BatchUtils.getBatchPolicyFilterExpForReactive(query, templateContext);

        return Flux.fromIterable(ids)
            .map(id -> getKey(id, setName, templateContext))
            .flatMap(key -> BatchUtils.getFromClientReactively(batchPolicy, key, targetClass, templateContext))
            .filter(keyRecord -> nonNull(keyRecord.record));
    }

    static <T> Comparator<T> getComparator(Query query) {
        return query.getSort().stream()
            .map(TemplateUtils::<T>getPropertyComparator)
            .reduce(Comparator::thenComparing)
            .orElseThrow(() -> new IllegalStateException("Comparator can not be created if sort orders are empty"));
    }

    static <T> Comparator<T> getComparator(Sort sort) {
        return sort.stream()
            .map(TemplateUtils::<T>getPropertyComparator)
            .reduce(Comparator::thenComparing)
            .orElseThrow(() -> new IllegalStateException("Comparator can not be created if sort orders are empty"));
    }

    private static <T> Comparator<T> getPropertyComparator(Sort.Order order) {
        boolean ignoreCase = true;
        boolean ascending = order.getDirection().isAscending();
        return new PropertyComparator<>(order.getProperty(), ignoreCase, ascending);
    }

    static <T> AerospikeWriteData writeData(T document, String setName, TemplateContext templateContext) {
        AerospikeWriteData data = AerospikeWriteData.forWrite(templateContext.namespace);
        data.setSetName(setName);
        templateContext.converter.write(document, data);
        return data;
    }

    static <T> AerospikeWriteData writeDataWithSpecificFields(T document, String setName, Collection<String> fields,
                                                              TemplateContext templateContext) {
        AerospikeWriteData data = AerospikeWriteData.forWrite(templateContext.namespace);
        data.setSetName(setName);
        data.setRequestedBins(MappingUtils.fieldsToBinNames(document, fields, templateContext));
        templateContext.converter.write(document, data);
        return data;
    }

    static Key getKey(Object id, AerospikePersistentEntity<?> entity, TemplateContext templateContext) {
        return getKey(id, entity.getSetName(), templateContext);
    }

    static Key getKey(Object id, String setName, TemplateContext templateContext) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Key key;
        // Choosing whether tp preserve id type based on the configuration
        if (templateContext.converter.getAerospikeDataSettings().isKeepOriginalKeyTypes()) {
            if (id instanceof Byte || id instanceof Short || id instanceof Integer || id instanceof Long) {
                key = new Key(
                    templateContext.namespace,
                    setName,
                    convertIfNecessary(templateContext.converter, ((Number) id).longValue(), Long.class)
                );
            } else if (id instanceof Character) {
                key = new Key(
                    templateContext.namespace,
                    setName,
                    convertIfNecessary(templateContext.converter, id, Character.class)
                );
            } else if (id instanceof byte[]) {
                key = new Key(
                    templateContext.namespace,
                    setName,
                    convertIfNecessary(templateContext.converter, id, byte[].class)
                );
            } else {
                key = new Key(
                    templateContext.namespace,
                    setName,
                    convertIfNecessary(templateContext.converter, id, String.class)
                );
            }
            return key;
        } else {
            return new Key(
                templateContext.namespace,
                setName,
                convertIfNecessary(templateContext.converter, id, String.class)
            );
        }
    }

    static Operation[] getPutAndGetHeaderOperations(AerospikeWriteData data, boolean firstlyDeleteBins) {
        Bin[] bins = data.getBinsAsArray();

        if (bins.length == 0) {
            throw new AerospikeException(
                "Cannot put and get header on a document with no bins and class bin disabled.");
        }

        return operations(bins, Operation::put, firstlyDeleteBins ? Operation.array(Operation.delete()) : null,
            Operation.array(Operation.getHeader()));
    }

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

    static Operation[] operations(Bin[] bins, Function<Bin, Operation> binToOperation) {
        return operations(bins, binToOperation, null, null);
    }

    static Operation[] operations(Bin[] bins, Function<Bin, Operation> binToOperation,
                                  Operation[] precedingOperations) {
        return operations(bins, binToOperation, precedingOperations, null);
    }

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
}
