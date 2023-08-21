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
import com.aerospike.client.Key;
import com.aerospike.client.Log;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
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
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    BaseAerospikeTemplate(String namespace,
                          MappingAerospikeConverter converter,
                          AerospikeMappingContext mappingContext,
                          AerospikeExceptionTranslator exceptionTranslator,
                          WritePolicy writePolicyDefault) {
        Assert.notNull(writePolicyDefault, "Write policy must not be null!");
        Assert.notNull(namespace, "Namespace cannot be null");
        Assert.hasLength(namespace, "Namespace cannot be empty");

        this.converter = converter;
        this.exceptionTranslator = exceptionTranslator;
        this.namespace = namespace;
        this.mappingContext = mappingContext;
        this.writePolicyDefault = writePolicyDefault;

        loggerSetup();
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
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
        return entity.getSetName();
    }

    public MappingContext<?, ?> getMappingContext() {
        return this.mappingContext;
    }

    public String getNamespace() {
        return namespace;
    }

    @SuppressWarnings("unchecked")
    <T> Class<T> getEntityClass(T entity) {
        return (Class<T>) entity.getClass();
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

    RuntimeException translateCasError(AerospikeException e) {
        int code = e.getResultCode();
        if (code == ResultCode.KEY_EXISTS_ERROR || code == ResultCode.GENERATION_ERROR) {
            return new OptimisticLockingFailureException("Save document with version value failed", e);
        }
        return translateError(e);
    }

    RuntimeException translateError(AerospikeException e) {
        DataAccessException translated = exceptionTranslator.translateExceptionIfPossible(e);
        return translated == null ? e : translated;
    }

    <T> AerospikeWriteData writeData(T document) {
        AerospikeWriteData data = AerospikeWriteData.forWrite(getNamespace());
        converter.write(document, data);
        return data;
    }

    <T> AerospikeWriteData writeDataWithSpecificFields(T document, Collection<String> fields) {
        AerospikeWriteData data = AerospikeWriteData.forWrite(getNamespace());
        data.setRequestedBins(fieldsToBinNames(document, fields));
        converter.write(document, data);
        return data;
    }

    WritePolicy expectGenerationCasAwareSavePolicy(AerospikeWriteData data) {
        RecordExistsAction recordExistsAction = data.getVersion()
            .filter(v -> v > 0L)
            .map(v -> RecordExistsAction.UPDATE_ONLY) // updating existing document with generation,
            // cannot use REPLACE_ONLY due to bin convergence feature restrictions
            .orElse(RecordExistsAction.CREATE_ONLY); // create new document,
        // if exists we should fail with optimistic locking
        return expectGenerationSavePolicy(data, recordExistsAction);
    }

    WritePolicy expectGenerationSavePolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction) {
        return WritePolicyBuilder.builder(this.writePolicyDefault)
            .generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
            .generation(data.getVersion().orElse(0))
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    WritePolicy ignoreGenerationSavePolicy(AerospikeWriteData data, RecordExistsAction recordExistsAction) {
        return WritePolicyBuilder.builder(this.writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .expiration(data.getExpiration())
            .recordExistsAction(recordExistsAction)
            .build();
    }

    WritePolicy ignoreGenerationDeletePolicy() {
        return WritePolicyBuilder.builder(this.writePolicyDefault)
            .generationPolicy(GenerationPolicy.NONE)
            .build();
    }

    Key getKey(Object id, AerospikePersistentEntity<?> entity) {
        Assert.notNull(id, "Id must not be null!");
        String userKey = convertIfNecessary(id, String.class);
        return new Key(this.namespace, entity.getSetName(), userKey);
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
}
