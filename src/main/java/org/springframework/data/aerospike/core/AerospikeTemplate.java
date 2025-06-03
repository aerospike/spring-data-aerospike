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
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
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
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.InfoCommandUtils;
import org.springframework.data.aerospike.util.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.*;
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
import static org.springframework.data.aerospike.core.BatchUtils.batchWriteSizeMatch;
import static org.springframework.data.aerospike.core.TemplateUtils.getDistinctPredicate;
import static org.springframework.data.aerospike.core.TemplateUtils.operations;
import static org.springframework.data.aerospike.core.ValidationUtils.verifyUnsortedWithOffset;
import static org.springframework.data.aerospike.core.MappingUtils.getBinNamesFromTargetClassOrNull;
import static org.springframework.data.aerospike.core.MappingUtils.getTargetClass;
import static org.springframework.data.aerospike.core.MappingUtils.mapToEntity;
import static org.springframework.data.aerospike.core.TemplateUtils.*;

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
    private final TemplateContext templateContext;

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
        this.templateContext = TemplateContext.builder()
            .client(client)
            .converter(converter)
            .namespace(namespace)
            .mappingContext(mappingContext)
            .exceptionTranslator(exceptionTranslator)
            .writePolicyDefault(writePolicyDefault)
            .batchWritePolicyDefault(batchWritePolicyDefault)
            .queryEngine(queryEngine)
            .build();
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

        AerospikeWriteData data = writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy = PolicyUtils.expectGenerationCasAwarePolicy(data,
                templateContext.writePolicyDefault);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            doPersistWithVersionAndHandleCasError(document, data, writePolicy, true, SAVE_OPERATION, templateContext);
        } else {
            WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.UPDATE,
                templateContext.writePolicyDefault);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
            doPersistAndHandleError(data, writePolicy, operations, templateContext);
        }
    }

    @Override
    public <T> void saveAll(Iterable<T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for saving");
            return;
        }
        saveAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void saveAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for saving");
            return;
        }
        BatchUtils.applyBufferedBatchWrite(documents, setName, SAVE_OPERATION, templateContext);
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

        AerospikeWriteData data = writeData(document, setName, templateContext);
        WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.CREATE_ONLY,
            templateContext.writePolicyDefault);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            // we are ignoring generation here as insert operation should fail with DuplicateKeyException if key
            // already exists
            // we do not mind which initial version is set in the document, BUT we need to update the version value
            // in the original document
            // also we do not want to handle aerospike error codes as cas aware error codes as we are ignoring
            // generation
            doPersistWithVersionAndHandleError(document, data, writePolicy, templateContext);
        } else {
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            doPersistAndHandleError(data, writePolicy, operations, templateContext);
        }
    }

    @Override
    public <T> void insertAll(Iterable<? extends T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for inserting");
            return;
        }
        insertAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void insertAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for inserting");
            return;
        }
        BatchUtils.applyBufferedBatchWrite(documents, setName, INSERT_OPERATION, templateContext);
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

        AerospikeWriteData data = writeData(document, setName, templateContext);

        Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
        // not using initial writePolicy instance because it can get enriched with transaction id
        doPersistAndHandleError(data, new WritePolicy(writePolicy), operations, templateContext);
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

        AerospikeWriteData data = writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy =
                PolicyUtils.expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                    templateContext.writePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            doPersistWithVersionAndHandleCasError(document, data, writePolicy, true, UPDATE_OPERATION, templateContext);
        } else {
            WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.writePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = Stream.concat(Stream.of(Operation.delete()), data.getBins().stream()
                .map(Operation::put)).toArray(Operation[]::new);
            doPersistAndHandleError(data, writePolicy, operations, templateContext);
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

        AerospikeWriteData data = writeDataWithSpecificFields(document, setName, fields, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy =
                PolicyUtils.expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                    templateContext.writePolicyDefault);

            doPersistWithVersionAndHandleCasError(document, data, writePolicy, false, UPDATE_OPERATION,
                templateContext);
        } else {
            WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.writePolicyDefault);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            doPersistAndHandleError(data, writePolicy, operations, templateContext);
        }
    }

    @Override
    public <T> void updateAll(Iterable<T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for updating");
            return;
        }
        updateAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> void updateAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for updating");
            return;
        }
        BatchUtils.applyBufferedBatchWrite(documents, setName, UPDATE_OPERATION, templateContext);
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

        AerospikeWriteData data = writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            return doDeleteWithVersionAndHandleCasError(data, templateContext);
        }
        return doDeleteIgnoreVersionAndTranslateError(data, templateContext);
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
            Key key = getKey(id, setName, templateContext);
            WritePolicy writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client,
                PolicyUtils.ignoreGenerationPolicy(templateContext.writePolicyDefault));
            return client.delete(writePolicy, key);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

        if (!findQueryResults.isEmpty()) {
            deleteAll(findQueryResults);
        }
    }

    @Override
    public <T> void deleteAll(Iterable<T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
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
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for deleting");
            return;
        }
        BatchUtils.applyBufferedBatchWrite(documents, setName, DELETE_OPERATION, templateContext);
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
        if (ValidationUtils.isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }
        BatchUtils.deleteByIds(ids, setName, false, templateContext);
    }

    @Override
    public void deleteExistingByIds(Iterable<?> ids, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return;
        }
        BatchUtils.deleteByIds(ids, setName, true, templateContext);
    }

    @Override
    public void deleteByIds(GroupedKeys groupedKeys) {
        if (ValidationUtils.areInvalidGroupedKeys(groupedKeys)) return;

        BatchUtils.deleteGroupedEntitiesByGroupedKeys(groupedKeys, templateContext);
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
        Calendar beforeLastUpdateCalendar = MappingUtils.convertToCalendar(beforeLastUpdate);

        try {
            client.truncate(null, getNamespace(), setName, beforeLastUpdateCalendar);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            AerospikeWriteData data = writeData(document, setName, templateContext);
            Operation[] ops = operations(values, Operation.Type.ADD, Operation.get());

            WritePolicy writePolicy = WritePolicyBuilder.builder(writePolicyDefault)
                .expiration(data.getExpiration())
                .build();
            writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client, writePolicy);

            Record aeroRecord = client.operate(writePolicy, data.getKey(), ops);

            return mapToEntity(data.getKey(), MappingUtils.getEntityClass(document), aeroRecord,
                templateContext.converter);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            AerospikeWriteData data = writeData(document, setName, templateContext);

            WritePolicy writePolicy = WritePolicyBuilder.builder(writePolicyDefault)
                .expiration(data.getExpiration())
                .build();
            writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client, writePolicy);

            Record aeroRecord = client.operate(writePolicy, data.getKey(),
                Operation.add(new Bin(binName, value)), Operation.get());

            return mapToEntity(data.getKey(), MappingUtils.getEntityClass(document), aeroRecord,
                templateContext.converter);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            AerospikeWriteData data = writeData(document, setName, templateContext);
            Operation[] ops = operations(values, Operation.Type.APPEND, Operation.get());
            WritePolicy writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client,
                client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(), ops);

            return mapToEntity(data.getKey(), MappingUtils.getEntityClass(document), aeroRecord,
                templateContext.converter);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            AerospikeWriteData data = writeData(document, setName, templateContext);
            WritePolicy writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client,
                client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(),
                Operation.append(new Bin(binName, value)),
                Operation.get(binName));

            return mapToEntity(data.getKey(), MappingUtils.getEntityClass(document), aeroRecord,
                templateContext.converter);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            AerospikeWriteData data = writeData(document, setName, templateContext);
            WritePolicy writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client,
                client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(),
                Operation.prepend(new Bin(fieldName, value)),
                Operation.get(fieldName));

            return mapToEntity(data.getKey(), MappingUtils.getEntityClass(document), aeroRecord,
                templateContext.converter);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            AerospikeWriteData data = writeData(document, setName, templateContext);
            Operation[] ops = operations(values, Operation.Type.PREPEND, Operation.get());
            WritePolicy writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client,
                client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, data.getKey(), ops);

            return mapToEntity(data.getKey(), MappingUtils.getEntityClass(document), aeroRecord,
                templateContext.converter);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    @Override
    public <T> T execute(Supplier<T> supplier) {
        Assert.notNull(supplier, "Supplier must not be null!");

        try {
            return supplier.get();
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
                    (Stream<S>) findByIdsUsingQuery(idsList, entityClass, targetClass, setName, null)
                        .filter(Objects::nonNull)
                ).toList();
                idsList.clear();
            }
            idsList.add(id);
        }
        if (!idsList.isEmpty()) {
            result = Stream.concat(
                result.stream(),
                (Stream<S>) findByIdsUsingQuery(idsList, entityClass, targetClass, setName, null)
                    .filter(Objects::nonNull)
            ).toList();
        }

        return result;
    }

    @Override
    public GroupedEntities findByIds(GroupedKeys groupedKeys) {
        if (ValidationUtils.areInvalidGroupedKeys(groupedKeys)) return GroupedEntities.builder().build();

        return BatchUtils.findGroupedEntitiesByGroupedKeys(groupedKeys, templateContext);
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
            Key key = getKey(id, setName, templateContext);

            if (targetClass != null && targetClass != entityClass) {
                return getRecordMapToTargetClass(entity, key, targetClass, query, templateContext);
            }
            return mapToEntity(key, entityClass, getRecord(entity, key, query, templateContext),
                templateContext.converter);
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    @Override
    public <T, S> Stream<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                                Query query) {
        return findByIdsUsingQuery(ids, entityClass, targetClass, getSetName(entityClass), query);
    }

    @Override
    public <T, S> Stream<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                                String setName, Query query) {
        Stream<?> results = findByIdsWithoutPostProcessing(ids, entityClass, targetClass, setName, query);
        return PostProcessingUtils.applyPostProcessingOnResults(results, query);
    }

    public <T, S> Stream<?> findByIdsWithoutPostProcessing(Collection<?> ids, Class<T> entityClass,
                                                           Class<S> targetClass, Query query) {
        return findByIdsWithoutPostProcessing(ids, entityClass, targetClass, getSetName(entityClass), query);
    }

    public <T, S> Stream<?> findByIdsWithoutPostProcessing(Collection<?> ids, Class<T> entityClass,
                                                           Class<S> targetClass, String setName,
                                                           Query query) {
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(entityClass, "Entity class must not be null!");

        Key[] keys = MappingUtils.getKeys(ids, setName, templateContext);
        String[] binNames = getBinNamesFromTargetClassOrNull(entityClass, targetClass, mappingContext);
        Record[] records = BatchUtils.findByKeysUsingQuery(keys, binNames, setName, query, templateContext);

        return IntStream.range(0, keys.length)
            .mapToObj(index -> mapToEntity(keys[index], getTargetClass(entityClass, targetClass), records[index],
                templateContext.converter));
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

        return TemplateUtils.findWithPostProcessing(setName, targetClass, query, templateContext);
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

        return TemplateUtils.find(targetClass, setName, templateContext);
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

        return findWithPostProcessing(setName, targetClass, sort, offset, limit, templateContext);
    }

    @Override
    public <T, S> Stream<S> findUsingQueryWithoutPostProcessing(Class<T> entityClass, Class<S> targetClass,
                                                                Query query) {
        verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        return findUsingQueryWithDistinctPredicate(getSetName(entityClass), targetClass,
            getDistinctPredicate(query), query, templateContext);
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

        return findWithPostProcessing(setName, targetClass, sort, offset, limit, templateContext);
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
            Key key = getKey(id, setName, templateContext);

            WritePolicy writePolicy = (WritePolicy) PolicyUtils.enrichPolicyWithTransaction(client,
                client.copyWritePolicyDefault());
            Record aeroRecord = client.operate(writePolicy, key, Operation.getHeader());
            return aeroRecord != null;
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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

        return findExistingKeyRecordsUsingQuery(setName, query, templateContext).findAny().isPresent();
    }

    @Override
    public <T> boolean existsByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return existsByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public boolean existsByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        long findQueryResults = BatchUtils.findByIdsUsingQueryWithoutMapping(ids, setName, query, templateContext)
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
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    @Override
    public <T> long count(Query query, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return count(query, getSetName(entityClass));
    }

    @Override
    public long count(Query query, String setName) {
        return findExistingKeyRecordsUsingQuery(setName, query, templateContext).count();
    }

    @Override
    public <T> long countExistingByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return countExistingByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public long countExistingByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        return BatchUtils.findByIdsUsingQueryWithoutMapping(ids, setName, query, templateContext)
            .filter(Objects::nonNull)
            .count();
    }

    @Override
    public <T> long countByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return countByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public long countByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        return BatchUtils.findByIdsUsingQueryWithoutMapping(ids, setName, query, templateContext)
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
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
        return false;
    }
}
