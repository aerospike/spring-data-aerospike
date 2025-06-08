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
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.index.IndexesCacheRefresher;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.InfoCommandUtils;
import org.springframework.data.domain.Sort;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.aerospike.client.ResultCode.KEY_NOT_FOUND_ERROR;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.DELETE_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.INSERT_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.SAVE_OPERATION;
import static org.springframework.data.aerospike.core.BaseAerospikeTemplate.OperationType.UPDATE_OPERATION;
import static org.springframework.data.aerospike.core.BatchUtils.findByIdsWithoutPostProcessingReactively;
import static org.springframework.data.aerospike.core.TemplateUtils.getDistinctPredicate;
import static org.springframework.data.aerospike.core.TemplateUtils.operations;
import static org.springframework.data.aerospike.core.MappingUtils.getTargetClass;
import static org.springframework.data.aerospike.core.MappingUtils.mapToEntity;
import static org.springframework.data.aerospike.core.TemplateUtils.*;
import static org.springframework.data.aerospike.query.QualifierUtils.isQueryCriteriaNotNull;
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
    private final TemplateContext templateContext;

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
        this.templateContext = TemplateContext.builder()
            .reactorClient(reactorClient)
            .converter(converter)
            .namespace(namespace)
            .mappingContext(mappingContext)
            .exceptionTranslator(exceptionTranslator)
            .writePolicyDefault(writePolicyDefault)
            .batchWritePolicyDefault(batchWritePolicyDefault)
            .reactorQueryEngine(queryEngine)
            .build();
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
        AerospikeWriteData data = writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy = PolicyUtils.expectGenerationCasAwarePolicy(data,
                templateContext.writePolicyDefault);
            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));

            return doPersistWithVersionAndHandleCasErrorReactively(document, data, writePolicy, operations,
                SAVE_OPERATION,
                templateContext);
        } else {
            WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.UPDATE,
                templateContext.writePolicyDefault);
            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));

            return doPersistAndHandleErrorReactively(document, data, writePolicy, operations, templateContext);
        }
    }

    @Override
    public <T> Flux<T> saveAll(Iterable<T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for saving");
            return Flux.empty();
        }
        return saveAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> saveAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for saving");
            return Flux.empty();
        }
        return BatchUtils.applyReactiveBatchWriteInChunks(documents, setName, SAVE_OPERATION, templateContext);
    }

    @Override
    public <T> Mono<T> insert(T document) {
        return insert(document, getSetName(document));
    }

    @Override
    public <T> Mono<T> insert(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName, templateContext);
        WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.CREATE_ONLY,
            templateContext.writePolicyDefault);

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
            return doPersistWithVersionAndHandleErrorReactively(document, data, writePolicy, operations,
                templateContext);
        } else {
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            return doPersistAndHandleErrorReactively(document, data, writePolicy, operations, templateContext);
        }
    }

    @Override
    public <T> Flux<T> insertAll(Iterable<  T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for inserting");
            return Flux.empty();
        }
        return insertAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> insertAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for inserting");
            return Flux.empty();
        }
        return BatchUtils.applyReactiveBatchWriteInChunks(documents, setName, INSERT_OPERATION, templateContext);
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

        AerospikeWriteData data = writeData(document, setName, templateContext);

        Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
        // not using initial writePolicy instance because it can get enriched with transaction id
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, new WritePolicy(writePolicy))
            .flatMap(writePolicyEnriched ->
                doPersistAndHandleErrorReactively(document, data, (WritePolicy) writePolicyEnriched, operations,
                    templateContext));
    }

    @Override
    public <T> Mono<T> update(T document) {
        return update(document, getSetName(document));
    }

    @Override
    public <T> Mono<T> update(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy = PolicyUtils.expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.writePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()), Operation.array(Operation.getHeader()));
            return doPersistWithVersionAndHandleCasErrorReactively(document, data, writePolicy, operations,
                UPDATE_OPERATION, templateContext);
        } else {
            WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.writePolicyDefault);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            Operation[] operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
            return doPersistAndHandleErrorReactively(document, data, writePolicy, operations, templateContext);
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

        AerospikeWriteData data = writeDataWithSpecificFields(document, setName, fields, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            WritePolicy writePolicy = PolicyUtils.expectGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.writePolicyDefault);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put, null,
                Operation.array(Operation.getHeader()));
            return doPersistWithVersionAndHandleCasErrorReactively(document, data, writePolicy, operations,
                UPDATE_OPERATION, templateContext);
        } else {
            WritePolicy writePolicy = PolicyUtils.ignoreGenerationPolicy(data, RecordExistsAction.UPDATE_ONLY,
                templateContext.writePolicyDefault);

            Operation[] operations = operations(data.getBinsAsArray(), Operation::put);
            return doPersistAndHandleErrorReactively(document, data, writePolicy, operations, templateContext);
        }
    }

    @Override
    public <T> Flux<T> updateAll(Iterable<T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for updating");
            return Flux.empty();
        }
        return updateAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Flux<T> updateAll(Iterable<T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for updating");
            return Flux.empty();
        }
        return BatchUtils.applyReactiveBatchWriteInChunks(documents, setName, UPDATE_OPERATION, templateContext);
    }

    @Override
    public <T> Mono<Boolean> delete(T document) {
        return delete(document, getSetName(document));
    }

    @Override
    public <T> Mono<Boolean> delete(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(document, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName, templateContext);
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        if (entity.hasVersionProperty()) {
            return PolicyUtils.enrichPolicyWithTransaction(
                    reactorClient,
                    PolicyUtils.expectGenerationPolicy(data, templateContext.writePolicyDefault)
                )
                .flatMap(writePolicyEnriched -> reactorClient.delete((WritePolicy) writePolicyEnriched, data.getKey()))
                .hasElement()
                .onErrorMap(e -> ExceptionUtils.translateCasThrowable(e, DELETE_OPERATION.toString(), templateContext));
        }
        return PolicyUtils.enrichPolicyWithTransaction(
                reactorClient,
                PolicyUtils.ignoreGenerationPolicy(templateContext.writePolicyDefault)
            )
            .flatMap(writePolicyEnriched -> reactorClient.delete((WritePolicy) writePolicyEnriched, data.getKey()))
            .hasElement()
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
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

        return PolicyUtils.enrichPolicyWithTransaction(
                reactorClient,
                PolicyUtils.ignoreGenerationPolicy(templateContext.writePolicyDefault)
            )
            .flatMap(writePolicyEnriched ->
                reactorClient.delete((WritePolicy) writePolicyEnriched, getKey(id, setName, templateContext)))
            .map(k -> true)
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    @Override
    public <T> Mono<Void> deleteAll(Iterable<? extends T> documents) {
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for deleting");
            return Mono.empty();
        }
        return deleteAll(documents, getSetName(documents.iterator().next()));
    }

    @Override
    public <T> Mono<Void> deleteAll(Iterable<? extends T> documents, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(documents)) {
            logEmptyItems(log, "Documents for deleting");
            return Mono.empty();
        }
        return BatchUtils.applyReactiveBatchWriteInChunks(documents, setName, DELETE_OPERATION, templateContext).then();
    }

    @Override
    public <T> Mono<Void> deleteByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        if (ValidationUtils.isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return Mono.empty();
        }
        return deleteByIds(ids, getSetName(entityClass));
    }

    @Override
    public <T> Mono<Void> deleteExistingByIds(Iterable<?> ids, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        if (ValidationUtils.isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return Mono.empty();
        }
        return deleteExistingByIds(ids, getSetName(entityClass));
    }

    @Override
    public Mono<Void> deleteByIds(Iterable<?> ids, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return Mono.empty();
        }
        return BatchUtils.deleteByIdsReactively(ids, setName, false, templateContext);
    }

    @Override
    public Mono<Void> deleteExistingByIds(Iterable<?> ids, String setName) {
        Assert.notNull(setName, "Set name must not be null!");
        if (ValidationUtils.isEmpty(ids)) {
            logEmptyItems(log, "Ids for deleting");
            return Mono.empty();
        }
        return BatchUtils.deleteByIdsReactively(ids, setName, true, templateContext);
    }

    @Override
    public Mono<Void> deleteByIds(GroupedKeys groupedKeys) {
        if (ValidationUtils.areInvalidGroupedKeys(groupedKeys)) return Mono.empty();

        return BatchUtils.deleteEntitiesByGroupedKeysReactively(groupedKeys, templateContext);
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
        Calendar beforeLastUpdateCalendar = MappingUtils.convertToCalendar(beforeLastUpdate);

        try {
            return Mono.fromRunnable(
                () -> reactorClient.getAerospikeClient().truncate(null, namespace, setName, beforeLastUpdateCalendar));
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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

        AerospikeWriteData data = writeData(document, setName, templateContext);

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

        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                executeOperationsReactivelyOnValue(document, data, (WritePolicy) writePolicyEnriched, operations,
                    templateContext));
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

        AerospikeWriteData data = writeData(document, setName, templateContext);

        WritePolicy writePolicy = WritePolicyBuilder.builder(writePolicyDefault)
            .expiration(data.getExpiration())
            .build();

        Operation[] operations = {Operation.add(new Bin(binName, value)), Operation.get(binName)};
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, writePolicy)
            .flatMap(writePolicyEnriched ->
                executeOperationsReactivelyOnValue(document, data, (WritePolicy) writePolicyEnriched, operations,
                    templateContext));
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

        AerospikeWriteData data = writeData(document, setName, templateContext);
        Operation[] operations = operations(values, Operation.Type.APPEND, Operation.get());
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                .copyWritePolicyDefault())
            .flatMap(writePolicyEnriched ->
                executeOperationsReactivelyOnValue(document, data, (WritePolicy) writePolicyEnriched, operations,
                    templateContext));
    }

    @Override
    public <T> Mono<T> append(T document, String binName, String value) {
        return append(document, getSetName(document), binName, value);
    }

    @Override
    public <T> Mono<T> append(T document, String setName, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName, templateContext);
        Operation[] operations = {Operation.append(new Bin(binName, value)), Operation.get(binName)};
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                .copyWritePolicyDefault())
            .flatMap(writePolicyEnriched ->
                executeOperationsReactivelyOnValue(document, data, (WritePolicy) writePolicyEnriched, operations,
                    templateContext));
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

        AerospikeWriteData data = writeData(document, setName, templateContext);
        Operation[] operations = operations(values, Operation.Type.PREPEND, Operation.get());
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                .copyWritePolicyDefault())
            .flatMap(writePolicyEnriched -> executeOperationsReactivelyOnValue(document, data,
                (WritePolicy) writePolicyEnriched, operations, templateContext));
    }

    @Override
    public <T> Mono<T> prepend(T document, String binName, String value) {
        return prepend(document, getSetName(document), binName, value);
    }

    @Override
    public <T> Mono<T> prepend(T document, String setName, String binName, String value) {
        Assert.notNull(document, "Document must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        AerospikeWriteData data = writeData(document, setName, templateContext);
        Operation[] operations = {Operation.prepend(new Bin(binName, value)), Operation.get(binName)};
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                .copyWritePolicyDefault())
            .flatMap(writePolicyEnriched ->
                executeOperationsReactivelyOnValue(document, data, (WritePolicy) writePolicyEnriched, operations,
                    templateContext));
    }

    @Override
    public <T> Mono<T> execute(Supplier<T> supplier) {
        Assert.notNull(supplier, "Supplier must not be null!");

        return Mono.fromSupplier(supplier)
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
    }

    @Override
    public <T> Mono<T> findById(Object id, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");
        return findById(id, entityClass, getSetName(entityClass));
    }

    @Override
    public <T> Mono<T> findById(Object id, Class<T> entityClass, String setName) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        Key key = getKey(id, setName, templateContext);

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(),
                "Touch on read is not supported for entity without expiration property");
            return touchAndGetReactively(key, entity.getExpiration(), null, null, templateContext)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record, templateContext.converter))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
        } else {
            return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                    .copyReadPolicyDefault())
                .flatMap(policy -> reactorClient.get(policy, key))
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, entityClass, keyRecord.record, templateContext.converter))
                .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
        }
    }

    @Override
    public <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass) {
        return findById(id, entityClass, targetClass, getSetName(entityClass));
    }

    @Override
    public <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass, String setName) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        Key key = getKey(id, setName, templateContext);

        String[] binNames = MappingUtils.getBinNamesFromTargetClass(targetClass, templateContext.mappingContext);

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(),
                "Touch on read is not supported for entity without expiration property");
            return touchAndGetReactively(key, entity.getExpiration(), binNames, null, templateContext)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, targetClass, keyRecord.record, templateContext.converter))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
        } else {
            return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                    .copyReadPolicyDefault())
                .flatMap(policy -> reactorClient.get(policy, key, binNames))
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, targetClass, keyRecord.record, templateContext.converter))
                .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
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
        Assert.notNull(ids, "Ids must not be null!");
        Assert.notNull(targetClass, "Class must not be null!");
        Assert.notNull(setName, "Set name must not be null!");

        //noinspection unchecked
        return (Flux<T>) findByIdsWithoutPostProcessingReactively(iterableToList(ids), targetClass, null, setName,
            null, templateContext);
    }

    @Override
    public Mono<GroupedEntities> findByIds(GroupedKeys groupedKeys) {
        if (ValidationUtils.areInvalidGroupedKeys(groupedKeys)) return Mono.just(GroupedEntities.builder().build());

        return BatchUtils.findGroupedEntitiesByGroupedKeysReactively(reactorClient.getAerospikeClient()
                .copyBatchPolicyDefault(),
            groupedKeys, templateContext);
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
        Key key = getKey(id, setName, templateContext);
        String[] binNames = MappingUtils.getBinNamesFromTargetClass(targetClass, templateContext.mappingContext);
        final Class<?> target = getTargetClass(entityClass, targetClass);

        if (entity.isTouchOnRead()) {
            Assert.state(!entity.hasExpirationProperty(),
                "Touch on read is not supported for entity without expiration property");
            return touchAndGetReactively(key, entity.getExpiration(), binNames, query, templateContext)
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record, templateContext.converter))
                .onErrorResume(
                    th -> th instanceof AerospikeException ae && ae.getResultCode() == KEY_NOT_FOUND_ERROR,
                    th -> Mono.empty()
                )
                .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
        } else {
            Policy policy = null;
            if (isQueryCriteriaNotNull(query)) {
                policy = reactorClient.getAerospikeClient().copyReadPolicyDefault();
                Qualifier qualifier = query.getCriteriaObject();
                policy.filterExp = reactorQueryEngine.getFilterExpressionsBuilder().build(qualifier);
            }
            return PolicyUtils.enrichPolicyWithTransaction(reactorClient, policy)
                .flatMap(rPolicy -> reactorClient.get(rPolicy, key, binNames))
                .filter(keyRecord -> Objects.nonNull(keyRecord.record))
                .map(keyRecord -> mapToEntity(keyRecord.key, target, keyRecord.record, templateContext.converter))
                .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
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
        Flux<?> results =
            findByIdsWithoutPostProcessingReactively(ids, entityClass, targetClass, setName, query, templateContext);
        return PostProcessingUtils.applyPostProcessingOnResults(results, query);
    }

    /**
     * Finds entities reactively by their IDs without post-processing.
     *
     * <p>This method retrieves a {@link Flux} of entities based on a collection of IDs, an entity class,
     * and an optional target class. It uses an optional {@link Query} to filter the results and does not apply
     * post-processing to the retrieved entities.</p>
     *
     * @param <T>         The type of the entity
     * @param <S>         The type of the target class to which the entities will be mapped
     * @param ids         A {@link Collection} of IDs of the entities to find
     * @param entityClass The {@link Class} of the entity
     * @param targetClass The {@link Class} to map entities to and retrieve bin names from, can be {@code null}
     * @param query       A {@link Query} to apply additional filtering or criteria to the search, can be {@code null}
     * @return A {@link Flux} of entities, mapped to the entity or target class, without post-processing
     */
    public <T, S> Flux<?> findByIdsWithoutPostProcessing(Collection<?> ids, Class<T> entityClass,
                                                         @Nullable Class<S> targetClass, @Nullable Query query) {
        return findByIdsWithoutPostProcessingReactively(ids, entityClass, targetClass, getSetName(entityClass), query,
            templateContext);
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

        return findWithPostProcessingReactively(setName, targetClass, query, templateContext);
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
        return findReactively(setName, targetClass, templateContext);
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

        return findWithPostProcessingReactively(setName, targetClass, sort, offset, limit, templateContext);
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

        return findWithPostProcessingReactively(setName, targetClass, sort, offset, limit, templateContext);
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

        Key key = getKey(id, setName, templateContext);
        return PolicyUtils.enrichPolicyWithTransaction(reactorClient, reactorClient.getAerospikeClient()
                .copyReadPolicyDefault())
            .flatMap(policy -> reactorClient.exists(policy, key))
            .map(Objects::nonNull)
            .defaultIfEmpty(false)
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
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
        return findExistingKeyRecordsUsingQueryReactively(setName, query, templateContext).hasElements();
    }

    @Override
    public <T> Mono<Boolean> existsByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return existsByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public Mono<Boolean> existsByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        return BatchUtils.findByIdsUsingQueryWithoutEntityMappingReactively(ids, setName, query, templateContext)
            .filter(keyRecord -> keyRecord != null && keyRecord.record != null)
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
            return Mono.fromCallable(() -> countSet(setName, templateContext));
        } catch (AerospikeException e) {
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
        }
    }

    @Override
    public <T> Mono<Long> countExistingByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query) {
        return countExistingByIdsUsingQuery(ids, getSetName(entityClass), query);
    }

    @Override
    public Mono<Long> countExistingByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query) {
        return BatchUtils.findByIdsUsingQueryWithoutEntityMappingReactively(ids, setName, query, templateContext)
            .filter(keyRecord -> keyRecord != null && keyRecord.record != null)
            .count();
    }

    @Override
    public <T> Mono<Long> count(Query query, Class<T> entityClass) {
        Assert.notNull(entityClass, "Class must not be null!");

        return count(query, getSetName(entityClass));
    }

    @Override
    public Mono<Long> count(Query query, String setName) {
        Assert.notNull(setName, "Set for count must not be null!");

        return findExistingKeyRecordsUsingQueryReactively(setName, query, templateContext).count();
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
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
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
            .onErrorMap(e -> ExceptionUtils.translateError(e, templateContext.exceptionTranslator));
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
            throw ExceptionUtils.translateError(e, templateContext.exceptionTranslator);
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

    @Override
    public <T, S> Flux<S> findUsingQueryWithoutPostProcessing(Class<T> entityClass, Class<S> targetClass, Query query) {
        ValidationUtils.verifyUnsortedWithOffset(query.getSort(), query.getOffset());
        return findUsingQueryWithDistinctPredicateReactively(getSetName(entityClass), targetClass,
            getDistinctPredicate(query), query, templateContext);
    }
}
