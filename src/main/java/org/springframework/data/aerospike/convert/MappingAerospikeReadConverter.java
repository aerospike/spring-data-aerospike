/*
 * Copyright 2018-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.InstanceCreatorMetadata;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.EntityInstantiator;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.springframework.data.aerospike.convert.AerospikeMetaData.PRIMARY_KEY;
import static org.springframework.data.aerospike.utility.TimeUtils.offsetInSecondsToUnixTime;

public class MappingAerospikeReadConverter implements EntityReader<Object, AerospikeReadData> {

    private final EntityInstantiators entityInstantiators;
    private final TypeAliasAccessor<Map<String, Object>> typeAliasAccessor;
    private final TypeMapper<Map<String, Object>> typeMapper;
    private final AerospikeMappingContext mappingContext;
    private final CustomConversions conversions;
    private final GenericConversionService conversionService;

    public MappingAerospikeReadConverter(EntityInstantiators entityInstantiators,
                                         TypeAliasAccessor<Map<String, Object>> typeAliasAccessor,
                                         TypeMapper<Map<String, Object>> typeMapper,
                                         AerospikeMappingContext mappingContext, CustomConversions conversions,
                                         GenericConversionService conversionService) {
        this.entityInstantiators = entityInstantiators;
        this.typeAliasAccessor = typeAliasAccessor;
        this.typeMapper = typeMapper;
        this.mappingContext = mappingContext;
        this.conversions = conversions;
        this.conversionService = conversionService;
    }

    private static Collection<?> asCollection(Object source) {
        if (source instanceof Collection) {
            return (Collection<?>) source;
        }
        return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
    }

    @Override
    public <R> R read(Class<R> targetClass, final AerospikeReadData data) {
        if (data == null) {
            return null;
        }

        Map<String, Object> aeroRecord = data.getAeroRecord();
        TypeInformation<? extends R> typeToUse =
            typeMapper.readType(aeroRecord, TypeInformation.of(targetClass));
        Class<? extends R> rawType = typeToUse.getType();
        if (conversions.hasCustomReadTarget(AerospikeReadData.class, rawType)) {
            return conversionService.convert(data, rawType);
        }

        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(typeToUse);
        RecordReadingPropertyValueProvider propertyValueProvider = new RecordReadingPropertyValueProvider(data);
        ConvertingPropertyAccessor<?> accessor = getConvertingPropertyAccessor(entity, propertyValueProvider);

        return convertProperties(entity, propertyValueProvider, accessor);
    }

    @SuppressWarnings("unchecked")
    private <T> T getIdValue(Key key, Map<String, Object> data, AerospikePersistentProperty property) {
        Value userKey = key.userKey;
        Object value = userKey == null ? data.get(PRIMARY_KEY) : userKey.getObject();
        Assert.notNull(value, "Id must not be null!");
        return (T) convertIfNeeded(value, property.getType());
    }

    @SuppressWarnings("unchecked")
    private <R> R convertProperties(AerospikePersistentEntity<?> entity,
                                    RecordReadingPropertyValueProvider propertyValueProvider,
                                    PersistentPropertyAccessor<?> accessor) {
        entity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) persistentProperty -> {

            InstanceCreatorMetadata<?> creatorMetadata = entity.getInstanceCreatorMetadata();
            if (creatorMetadata != null && creatorMetadata.isCreatorParameter(persistentProperty)) {
                return;
            }

            Object value = propertyValueProvider.getPropertyValue(persistentProperty);
            if (persistentProperty.getType().isPrimitive() && value == null) {
                return;
            }

            accessor.setProperty(persistentProperty, value);
        });

        return (R) accessor.getBean();
    }

    @SuppressWarnings("unchecked")
    private <T> T readValue(Object source, TypeInformation<?> propertyType) {
        Assert.notNull(propertyType, "Target type must not be null!");

        if (source == null) {
            return null;
        }
        Class<?> targetClass = propertyType.getType();
        if (conversions.hasCustomReadTarget(source.getClass(), targetClass)) {
            return (T) conversionService.convert(source, targetClass);
        } else if (propertyType.isCollectionLike()) {
            /*
             * Byte arrays should not be converted or waste time on unnecessary convert collection flow -
             * if the source type is byte[] and the target class is also byte[].
             * If target is a List<Byte> then convert as a collection.
             */
            if (source instanceof byte[] && targetClass == byte[].class) {
                return (T) source;
            }
            return convertCollection(asCollection(source), propertyType);
        } else if (propertyType.isMap()) {
            return convertMap((Map<String, Object>) source, propertyType);
        } else if (source instanceof Map) { // custom type
            return convertCustomType((Map<String, Object>) source, propertyType);
        }
        return (T) convertIfNeeded(source, targetClass);
    }

    @SuppressWarnings("unchecked")
    private <T> T convertCustomType(Map<String, Object> source, TypeInformation<?> propertyType) {
        TypeInformation<?> typeToUse = typeMapper.readType(source, propertyType);
        AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);
        if (shouldDefaultToMap(source, entity)) {
            return (T) source;
        }
        RecordReadingPropertyValueProvider propertyValueProvider = new RecordReadingPropertyValueProvider(source);
        PersistentPropertyAccessor<?> persistentPropertyAccessor =
            getConvertingPropertyAccessor(entity, propertyValueProvider);
        return convertProperties(entity, propertyValueProvider, persistentPropertyAccessor);
    }

    private boolean shouldDefaultToMap(Map<String, Object> source, AerospikePersistentEntity<?> entity) {
        return entity == null && !typeAliasAccessor.readAliasFrom(source).isPresent();
    }

    @SuppressWarnings("unchecked")
    private <R> R convertMap(Map<String, Object> source, TypeInformation<?> propertyType) {
        Class<?> mapClass = propertyType.getType();
        TypeInformation<?> keyType = propertyType.getComponentType();
        Class<?> keyClass = keyType == null ? null : keyType.getType();
        TypeInformation<?> mapValueType = propertyType.getMapValueType();

        Map<Object, Object> converted = CollectionFactory.createMap(mapClass, keyClass, source.keySet().size());

        source.forEach((k, v) -> {
            Object key = keyClass != null ? conversionService.convert(k, keyClass) : k;
            Object value = readValue(v, mapValueType);
            converted.put(key, value);
        });

        return (R) convertIfNeeded(converted, propertyType.getType());
    }

    @SuppressWarnings("unchecked")
    private <R> R convertCollection(final Collection<?> source, final TypeInformation<?> propertyType) {
        Class<?> collectionClass = propertyType.getType();
        TypeInformation<?> elementType = propertyType.getComponentType();
        Class<?> elementClass = elementType == null ? null : elementType.getType();

        Collection<Object> items = collectionClass.isArray() ? new ArrayList<>() :
            CollectionFactory.createCollection(collectionClass, elementClass, source.size());

        source.forEach(item -> items.add(readValue(item, elementType)));

        return (R) convertIfNeeded(items, propertyType.getType());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object convertIfNeeded(Object value, Class<?> targetClass) {
        if (Enum.class.isAssignableFrom(targetClass)) {
            return Enum.valueOf((Class<Enum>) targetClass, value.toString());
        }
        return targetClass.isAssignableFrom(value.getClass()) ? value : conversionService.convert(value, targetClass);
    }

    private ConvertingPropertyAccessor<Object> getConvertingPropertyAccessor(
        AerospikePersistentEntity<?> entity,
        RecordReadingPropertyValueProvider recordReadingPropertyValueProvider) {
        EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);
        Object instance = instantiator.createInstance(entity, new PersistentEntityParameterValueProvider<>(entity,
            recordReadingPropertyValueProvider, null));

        return new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(instance), conversionService);
    }

    @SuppressWarnings("unchecked")
    private <T> T getExpiration(int expiration, AerospikePersistentProperty property) {
        if (property.isExpirationSpecifiedAsUnixTime()) {
            return (T) convertIfNeeded(offsetInSecondsToUnixTime(expiration), property.getType());
        }
        return (T) convertIfNeeded(expiration, property.getType());
    }

    @SuppressWarnings("unchecked")
    private <T> T getVersion(int generation, AerospikePersistentProperty property) {
        return (T) convertIfNeeded(generation, property.getType());
    }

    /**
     * A {@link PropertyValueProvider} to lookup values on the configured {@link Record}.
     *
     * @author Oliver Gierke
     */
    private class RecordReadingPropertyValueProvider implements PropertyValueProvider<AerospikePersistentProperty> {

        private final Key key;
        private final Integer expiration;
        private final int generation;
        private final Map<String, Object> source;

        public RecordReadingPropertyValueProvider(AerospikeReadData readData) {
            this(readData.getKey(), readData.getExpiration(), readData.getVersion(), readData.getAeroRecord());
        }

        public RecordReadingPropertyValueProvider(Map<String, Object> source) {
            this(null, null, 0, source);
        }

        public RecordReadingPropertyValueProvider(Key key, Integer expiration, int generation,
                                                  Map<String, Object> source) {
            this.key = key;
            this.expiration = expiration;
            this.generation = generation;
            this.source = source;
        }

        @Override
        public <T> T getPropertyValue(AerospikePersistentProperty property) {
            if (key != null && property.isIdProperty()) {
                return getIdValue(key, source, property);
            }
            if (expiration != null && property.isExpirationProperty()) {
                return getExpiration(expiration, property);
            }
            if (property.isVersionProperty()) {
                // version of the document gets updated on save, so we do expect an accessor to be present
                return getVersion(generation, property);
            }
            Object value = source.get(property.getFieldName());

            return readValue(value, property.getTypeInformation());
        }
    }
}
