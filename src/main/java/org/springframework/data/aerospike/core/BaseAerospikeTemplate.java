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
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.Calendar;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.CoreUtils.operations;

/**
 * Base class for creation Aerospike templates
 *
 * @author Anastasiia Smirnova
 * @author Igor Ermolenko
 */
@Slf4j
abstract class BaseAerospikeTemplate {

    protected final MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;
    protected final MappingAerospikeConverter converter;
    protected final String namespace;
    protected final AerospikeExceptionTranslator exceptionTranslator;
    protected final WritePolicy writePolicyDefault;
    protected final BatchWritePolicy batchWritePolicyDefault;
    protected final ServerVersionSupport serverVersionSupport;

    BaseAerospikeTemplate(String namespace,
                          MappingAerospikeConverter converter,
                          AerospikeMappingContext mappingContext,
                          AerospikeExceptionTranslator exceptionTranslator,
                          WritePolicy writePolicyDefault,
                          ServerVersionSupport serverVersionSupport) {
        Assert.notNull(writePolicyDefault, "Write policy must not be null!");
        Assert.notNull(namespace, "Namespace cannot be null");
        Assert.hasLength(namespace, "Namespace cannot be empty");

        this.converter = converter;
        this.exceptionTranslator = exceptionTranslator;
        this.namespace = namespace;
        this.mappingContext = mappingContext;
        this.writePolicyDefault = writePolicyDefault;
        this.batchWritePolicyDefault = getFromWritePolicy(writePolicyDefault);
        this.serverVersionSupport = serverVersionSupport;

        loggerSetup();
    }

    private BatchWritePolicy getFromWritePolicy(WritePolicy writePolicy) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
        batchWritePolicy.commitLevel = writePolicy.commitLevel;
        batchWritePolicy.durableDelete = writePolicy.durableDelete;
        batchWritePolicy.generationPolicy = writePolicy.generationPolicy;
        batchWritePolicy.expiration = writePolicy.expiration;
        batchWritePolicy.sendKey = writePolicy.sendKey;
        batchWritePolicy.recordExistsAction = writePolicy.recordExistsAction;
        batchWritePolicy.filterExp = writePolicy.filterExp;
        return batchWritePolicy;
    }

    private void loggerSetup() {
        Logger log = LoggerFactory.getLogger("com.aerospike.client");
        Log.setCallback((level, message) -> {
            switch (level) {
                case INFO -> log.info("{}", message);
                case DEBUG -> log.debug("{}", message);
                case ERROR -> log.error("{}", message);
                case WARN -> log.warn("{}", message);
            }
        });
    }

    public <T> String getSetName(Class<T> entityClass) {
        return mappingContext.getRequiredPersistentEntity(entityClass).getSetName();
    }

    public <T> String getSetName(T document) {
        return mappingContext.getRequiredPersistentEntity(document.getClass()).getSetName();
    }

    public MappingContext<?, ?> getMappingContext() {
        return this.mappingContext;
    }

    public MappingAerospikeConverter getAerospikeConverter() {
        return this.converter;
    }

    public String getNamespace() {
        return namespace;
    }

    @SuppressWarnings("unchecked")
    <T> Class<T> getEntityClass(T entity) {
        return (Class<T>) entity.getClass();
    }

    <T> T mapToEntity(KeyRecord keyRecord, Class<T> targetClass) {
        return mapToEntity(keyRecord.key, targetClass, keyRecord.record);
    }

    <T> T mapToEntity(Key key, Class<T> clazz, Record aeroRecord) {
        if (aeroRecord == null) {
            return null;
        }
        AerospikeReadData data = AerospikeReadData.forRead(key, aeroRecord);
        return converter.read(clazz, data);
    }

    protected <T> Comparator<T> getComparator(Query query) {
        return query.getSort().stream()
            .map(this::<T>getPropertyComparator)
            .reduce(Comparator::thenComparing)
            .orElseThrow(() -> new IllegalStateException("Comparator can not be created if sort orders are empty"));
    }

    protected <T> Comparator<T> getComparator(Sort sort) {
        return sort.stream()
            .map(this::<T>getPropertyComparator)
            .reduce(Comparator::thenComparing)
            .orElseThrow(() -> new IllegalStateException("Comparator can not be created if sort orders are empty"));
    }

    private <T> Comparator<T> getPropertyComparator(Sort.Order order) {
        boolean ignoreCase = true;
        boolean ascending = order.getDirection().isAscending();
        return new PropertyComparator<>(order.getProperty(), ignoreCase, ascending);
    }

    <T> ConvertingPropertyAccessor<T> getPropertyAccessor(AerospikePersistentEntity<?> entity, T source) {
        PersistentPropertyAccessor<T> accessor = entity.getPropertyAccessor(source);
        return new ConvertingPropertyAccessor<>(accessor, converter.getConversionService());
    }

    <T> T updateVersion(T document, Record newAeroRecord) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        ConvertingPropertyAccessor<T> propertyAccessor = getPropertyAccessor(entity, document);
        AerospikePersistentProperty versionProperty = entity.getRequiredVersionProperty();
        propertyAccessor.setProperty(versionProperty, newAeroRecord.generation);
        return document;
    }

    RuntimeException translateCasError(AerospikeException e, String errMsg) {
        if (hasOptimisticLockingError(e.getResultCode())) {
            return getOptimisticLockingFailureException(errMsg, e);
        }
        return translateError(e);
    }

    protected boolean hasOptimisticLockingError(int resultCode) {
        return List.of(ResultCode.GENERATION_ERROR, ResultCode.KEY_EXISTS_ERROR, ResultCode.KEY_NOT_FOUND_ERROR)
            .contains(resultCode);
    }

    protected OptimisticLockingFailureException getOptimisticLockingFailureException(String errMsg,
                                                                                     AerospikeException e) {
        return new OptimisticLockingFailureException(errMsg, e);
    }

    RuntimeException translateError(AerospikeException e) {
        DataAccessException translated = exceptionTranslator.translateExceptionIfPossible(e);
        return translated == null ? e : translated;
    }

    <T> AerospikeWriteData writeData(T document, String setName) {
        AerospikeWriteData data = AerospikeWriteData.forWrite(getNamespace());
        data.setSetName(setName);
        converter.write(document, data);
        return data;
    }

    <T> AerospikeWriteData writeDataWithSpecificFields(T document, String setName, Collection<String> fields) {
        AerospikeWriteData data = AerospikeWriteData.forWrite(getNamespace());
        data.setSetName(setName);
        data.setRequestedBins(fieldsToBinNames(document, fields));
        converter.write(document, data);
        return data;
    }

    WritePolicy expectGenerationCasAwarePolicy(AerospikeWriteData data) {
        RecordExistsAction recordExistsAction = data.getVersion()
            .filter(v -> v > 0L)
            .map(v -> RecordExistsAction.UPDATE_ONLY) // updating existing document with generation,
            // cannot use REPLACE_ONLY due to bin convergence feature restrictions
            .orElse(RecordExistsAction.CREATE_ONLY); // create new document,
        // if exists we should fail with optimistic locking
        return expectGenerationPolicy(data, recordExistsAction);
    }

    BatchWritePolicy expectGenerationCasAwareBatchPolicy(AerospikeWriteData data) {
        RecordExistsAction recordExistsAction = data.getVersion()
            .filter(v -> v > 0L)
            .map(v -> RecordExistsAction.UPDATE_ONLY) // updating existing document with generation,
            // cannot use REPLACE_ONLY due to bin convergence feature restrictions
            .orElse(RecordExistsAction.CREATE_ONLY); // create new document,
        // if exists we should fail with optimistic locking
        return expectGenerationBatchPolicy(data, recordExistsAction);
    }

    WritePolicy expectGenerationPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction) {
        return WritePolicyBuilder.builder(this.writePolicyDefault)
            .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
            .generation(data.getVersion().orElse(0))
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    BatchWritePolicy expectGenerationBatchPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy(this.batchWritePolicyDefault);
        batchWritePolicy.generationPolicy = GenerationPolicy.EXPECT_GEN_EQUAL;
        batchWritePolicy.generation = data.getVersion().orElse(0);
        batchWritePolicy.expiration = data.getExpiration();
        batchWritePolicy.recordExistsAction = recordExistsAction;
        return batchWritePolicy;
    }

    WritePolicy ignoreGenerationPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction) {
        return WritePolicyBuilder.builder(this.writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    BatchWritePolicy ignoreGenerationBatchPolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy(this.batchWritePolicyDefault);
        batchWritePolicy.generationPolicy = GenerationPolicy.NONE;
        batchWritePolicy.expiration = data.getExpiration();
        batchWritePolicy.recordExistsAction = recordExistsAction;
        return batchWritePolicy;
    }

    WritePolicy ignoreGenerationPolicy() {
        return WritePolicyBuilder.builder(this.writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .build();
    }

    WritePolicy expectGenerationPolicy(AerospikeWriteData data) {
        return WritePolicyBuilder.builder(this.writePolicyDefault)
            .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
            .generation(data.getVersion().orElse(0))
            .expiration(data.getExpiration())
            .build();
    }

    Key getKey(Object id, AerospikePersistentEntity<?> entity) {
        return getKey(id, entity.getSetName());
    }

    Key getKey(Object id, String setName) {
        Assert.notNull(id, "Id must not be null!");
        Assert.notNull(setName, "Set name must not be null!");
        Key key;
        // choosing whether tp preserve id type based on the configuration
        if (converter.getAerospikeDataSettings().isKeepOriginalKeyTypes()) {
            if (id instanceof Byte || id instanceof Short || id instanceof Integer || id instanceof Long) {
                key = new Key(this.namespace, setName, convertIfNecessary(((Number) id).longValue(), Long.class));
            } else if (id instanceof Character) {
                key = new Key(this.namespace, setName, convertIfNecessary(id, Character.class));
            } else if (id instanceof byte[]) {
                key = new Key(this.namespace, setName, convertIfNecessary(id, byte[].class));
            } else {
                key = new Key(this.namespace, setName, convertIfNecessary(id, String.class));
            }
            return key;
        } else {
            return new Key(this.namespace, setName, convertIfNecessary(id, String.class));
        }
    }

    GroupedEntities toGroupedEntities(EntitiesKeys entitiesKeys, Record[] records) {
        GroupedEntities.GroupedEntitiesBuilder builder = GroupedEntities.builder();

        IntStream.range(0, entitiesKeys.getKeys().length)
            .filter(index -> records[index] != null)
            .mapToObj(index -> mapToEntity(entitiesKeys.getKeys()[index], entitiesKeys.getEntityClasses()[index],
                records[index]))
            .filter(Objects::nonNull)
            .forEach(entity -> builder.entity(getEntityClass(entity), entity));

        return builder.build();
    }

    Map<Class<?>, List<Key>> toEntitiesKeyMap(GroupedKeys groupedKeys) {
        return groupedKeys.getEntitiesKeys().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry -> toKeysList(entry.getKey(), entry.getValue())));
    }

    private <T> List<String> fieldsToBinNames(T document, Collection<String> fields) {
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());

        return fields.stream()
            .map(field -> {
                // Field is a class member of document class.
                if (entity.getPersistentProperty(field) != null) {
                    return Objects.requireNonNull(entity.getPersistentProperty(field)).getFieldName();
                }
                // Field is a @Field annotated value (already a bin name).
                if (getFieldAnnotatedValue(entity, field) != null) {
                    return field;
                }
                throw translateError(new AerospikeException("Cannot convert field: " + field +
                    " to bin name. field doesn't exists."));
            })
            .collect(Collectors.toList());
    }

    private String getFieldAnnotatedValue(AerospikePersistentEntity<?> entity, String field) {
        for (AerospikePersistentProperty property : entity.getPersistentProperties(Field.class)) {
            if (property.getFieldName().equals(field)) {
                return field;
            }
        }
        return null;
    }

    private <T> List<Key> toKeysList(Class<T> entityClass, Collection<?> ids) {
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(ids, "List of ids must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        List<?> idsList = IterableConverter.toList(ids);

        return idsList.stream()
            .map(id -> getKey(id, entity))
            .collect(Collectors.toList());
    }

    @SuppressWarnings({"unchecked", "SameParameterValue"})
    private <S> S convertIfNecessary(Object source, Class<S> type) {
        return type.isAssignableFrom(source.getClass()) ? (S) source
            : converter.getConversionService().convert(source, type);
    }

    protected Instant convertToInstant(Long millis) {
        if (millis == null) return null;

        if (millis >= Instant.now().toEpochMilli())
            throw new IllegalArgumentException("Last update time (%d) must be less than the current time"
                .formatted(millis));
        return Instant.ofEpochMilli(millis);
    }

    protected Calendar convertToCalendar(Instant instant) {
        if (instant == null) return null;

        Calendar calendar = Calendar.getInstance();
        if (instant.toEpochMilli() > calendar.getTimeInMillis())
            throw new IllegalArgumentException("Last update time (%d) must be less than the current time"
                .formatted(instant.toEpochMilli()));
        calendar.setTime(Date.from(instant));
        return calendar;
    }

    protected Operation[] getPutAndGetHeaderOperations(AerospikeWriteData data, boolean firstlyDeleteBins) {
        Bin[] bins = data.getBinsAsArray();

        if (bins.length == 0) {
            throw new AerospikeException(
                "Cannot put and get header on a document with no bins and \"@_class\" bin disabled.");
        }

        return operations(bins, Operation::put, firstlyDeleteBins ? Operation.array(Operation.delete()) : null,
            Operation.array(Operation.getHeader()));
    }

    public <T> BatchWriteData<T> getBatchWriteForSave(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy = expectGenerationCasAwareBatchPolicy(data);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = getPutAndGetHeaderOperations(data, true);
        } else {
            policy = ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE);

            // mimicking REPLACE behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = operations(data.getBinsAsArray(), Operation::put,
                Operation.array(Operation.delete()));
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    public <T> BatchWriteData<T> getBatchWriteForInsert(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy = ignoreGenerationBatchPolicy(data, RecordExistsAction.CREATE_ONLY);
        if (entity.hasVersionProperty()) {
            operations = getPutAndGetHeaderOperations(data, false);
        } else {
            operations = operations(data.getBinsAsArray(), Operation::put);
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    public <T> BatchWriteData<T> getBatchWriteForUpdate(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");

        AerospikeWriteData data = writeData(document, setName);

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy;
        if (entity.hasVersionProperty()) {
            policy = expectGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = getPutAndGetHeaderOperations(data, true);
        } else {
            policy = ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY);

            // mimicking REPLACE_ONLY behavior by firstly deleting bins due to bin convergence feature restrictions
            operations = Stream.concat(Stream.of(Operation.delete()), data.getBins().stream()
                .map(Operation::put)).toArray(Operation[]::new);
        }

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    public <T> BatchWriteData<T> getBatchWriteForDelete(T document, String setName) {
        Assert.notNull(document, "Document must not be null!");

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
        Operation[] operations;
        BatchWritePolicy policy;
        AerospikeWriteData data = writeData(document, setName);

        if (entity.hasVersionProperty()) {
            policy = expectGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY);
        } else {
            policy = ignoreGenerationBatchPolicy(data, RecordExistsAction.UPDATE_ONLY);
        }
        operations = Operation.array(Operation.delete());

        return new BatchWriteData<>(document, new BatchWrite(policy, data.getKey(), operations),
            entity.hasVersionProperty());
    }

    protected void validateGroupedKeys(GroupedKeys groupedKeys) {
        Assert.notNull(groupedKeys, "Grouped keys must not be null!");
        validateForBatchWrite(groupedKeys.getEntitiesKeys(), "Entities keys");
    }

    protected void validateForBatchWrite(Object object, String objectName) {
        Assert.notNull(object, objectName + " must not be null!");
        Assert.isTrue(batchWriteSupported(), "Batch write operations are supported starting with " +
            "server version " + TemplateUtils.SERVER_VERSION_6);
    }

    protected boolean batchWriteSizeMatch(int batchSize, int currentSize) {
        return batchSize > 0 && currentSize == batchSize;
    }

    protected boolean batchRecordFailed(BatchRecord batchRecord) {
        return batchRecord.resultCode != ResultCode.OK || batchRecord.record == null;
    }

    protected boolean batchWriteSupported() {
        return serverVersionSupport.batchWrite();
    }

    protected enum OperationType {
        SAVE_OPERATION("save"),
        INSERT_OPERATION("insert"),
        UPDATE_OPERATION("update"),
        DELETE_OPERATION("delete");

        private final String name;

        OperationType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    protected record BatchWriteData<T>(T document, BatchRecord batchRecord, boolean hasVersionProperty) {

    }
}
