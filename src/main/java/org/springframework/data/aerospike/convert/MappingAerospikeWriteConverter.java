/*
 * Copyright 2012-2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.aerospike.client.ResultCode.OP_NOT_APPLICABLE;
import static org.springframework.data.aerospike.util.TimeUtils.unixTimeToOffsetInSeconds;

@Slf4j
public class MappingAerospikeWriteConverter implements EntityWriter<Object, AerospikeWriteData> {

    private final TypeMapper<Map<String, Object>> typeMapper;
    private final AerospikeMappingContext mappingContext;
    private final CustomConversions conversions;
    private final GenericConversionService conversionService;
    private final AerospikeDataSettings settings;

    public MappingAerospikeWriteConverter(TypeMapper<Map<String, Object>> typeMapper,
                                          AerospikeMappingContext mappingContext, CustomConversions conversions,
                                          GenericConversionService conversionService,
                                          AerospikeDataSettings settings) {
        this.typeMapper = typeMapper;
        this.mappingContext = mappingContext;
        this.conversions = conversions;
        this.conversionService = conversionService;
        this.settings = settings;
    }

    private static Collection<?> asCollection(final Object source) {
        if (source instanceof Collection) {
            return (Collection<?>) source;
        }
        return source.getClass().isArray() ? CollectionUtils.arrayToList(source) : Collections.singleton(source);
    }

    @Override
    public void write(Object source, final AerospikeWriteData data) {
        if (source == null) {
            return;
        }

        boolean hasCustomConverter = conversions.hasCustomWriteTarget(source.getClass(), AerospikeWriteData.class);
        if (hasCustomConverter) {
            convertToAerospikeWriteData(source, data);
            return;
        }

        TypeInformation<?> type = TypeInformation.of(source.getClass());
        AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(source.getClass());
        ConvertingPropertyAccessor<?> accessor =
            new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(source), conversionService);

        data.setKeyForWrite(getNewKey(data, accessor, entity));

        AerospikePersistentProperty versionProperty = entity.getVersionProperty();
        if (versionProperty != null) {
            Integer version = accessor.getProperty(versionProperty, Integer.class);
            data.setVersion(version);
        }

        data.setExpiration(getExpiration(entity, accessor));

        Map<String, Object> convertedProperties = convertProperties(type, entity, accessor, false);

        if (!data.hasRequestedBins()) {
            convertedProperties.forEach(data::addBin);
        } else {
            convertedProperties.forEach((key, value) -> {
                if (data.getRequestedBins().contains(key)) {
                    data.addBin(key, value);
                }
            });
        }
    }

    public Optional<Key> getNewKey(AerospikeWriteData data,
                                   ConvertingPropertyAccessor<?> accessor, AerospikePersistentEntity<?> entity) {
        // set the new key if the one in provided data is null or incomplete
        Key key = data.getKey();
        if (key == null || key.userKey.getObject() == null || key.userKey.getObject().toString().isEmpty()
            || key.setName == null || key.namespace == null) {
            AerospikePersistentProperty idProperty = entity.getIdProperty();
            if (idProperty != null) {
                String setName = Optional.ofNullable(data.getSetName()).orElse(entity.getSetName());

                // Store record key as it is (if Aerospike supports it natively and configured)
                if (settings.isKeepOriginalKeyTypes()
                    && isValidAerospikeRecordKeyType(idProperty.getType())) {
                    log.debug("Attempt to construct record key with original key type");
                    Object nativeTypeId = accessor.getProperty(idProperty, idProperty.getType());
                    Assert.notNull(nativeTypeId, "Id must not be null!");
                    Key aerospikeRecordKey = constructAerospikeRecordKey(data.getNamespace(), setName, nativeTypeId);
                    if (aerospikeRecordKey != null) {
                        return Optional.of(aerospikeRecordKey);
                    }
                }
                // Store record key as a String (Used for unsupported Aerospike key types and older versions)
                String stringId = accessor.getProperty(idProperty, String.class);
                Assert.notNull(stringId, "Id must not be null!");
                log.debug("Attempt to construct record key as String");
                return Optional.of(new Key(data.getNamespace(), setName, stringId));
            } else {
                // id is mandatory
                throw new AerospikeException(OP_NOT_APPLICABLE, "Id has not been provided");
            }
        }
        return Optional.empty();
    }

    private void convertToAerospikeWriteData(Object source, AerospikeWriteData data) {
        AerospikeWriteData converted = conversionService.convert(source, AerospikeWriteData.class);
        Assert.notNull(converted, "Converted AerospikeWriteData cannot be null");
        data.setBins(new ArrayList<>(converted.getBins()));
        data.setKey(converted.getKey());
        data.setExpiration(converted.getExpiration());
    }

    private Map<String, Object> convertProperties(TypeInformation<?> type, AerospikePersistentEntity<?> entity,
                                                  ConvertingPropertyAccessor<?> accessor, boolean isCustomType) {
        Map<String, Object> target;
        if (settings.isWriteTreeMaps()) {
            target = new TreeMap<>();
        } else {
            target = new HashMap<>();
        }
        typeMapper.writeType(type, target);
        entity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) property -> {

            Object value = accessor.getProperty(property);
			/*
				For custom type bins - for example a nested POJO (Person has a friend field which is also a Person),
				We want to keep non-writable types (@Id, @Expiration, @Version...) as they are.
				This is not relevant for records, only for custom type bins.
			 */
            if (isNotWritable(property) && !isCustomType) {
                return;
            }
            Object valueToWrite = getValueToWrite(value, property.getTypeInformation());
            if (valueToWrite != null) {
                target.put(property.getFieldName(), valueToWrite);
            }
        });
        return target;
    }

    private boolean isNotWritable(AerospikePersistentProperty property) {
        return property.isIdProperty() || property.isExpirationProperty() || property.isVersionProperty()
            || !property.isWritable();
    }

    Object getValueToWrite(Object value, TypeInformation<?> type) {
        if (value == null) {
            return null;
        } else if (type == null || isSimpleValue(value.getClass())) {
            return getSimpleValueToWrite(value);
        } else {
            return getNonSimpleValueToWrite(value, type);
        }
    }

    private boolean isSimpleValue(Class<?> clazz) {
        return conversions.isSimpleType(clazz) && (!clazz.isArray() || clazz == byte[].class);
    }

    private Object getSimpleValueToWrite(Object value) {
        Optional<Class<?>> customTarget = conversions.getCustomWriteTarget(value.getClass());
        return customTarget
            .<Object>map(aClass -> conversionService.convert(value, aClass))
            .orElse(value);
    }

    private Object getNonSimpleValueToWrite(Object value, TypeInformation<?> type) {
        TypeInformation<?> valueType = TypeInformation.of(value.getClass());

        if (valueType.isCollectionLike()) {
            return convertCollection(asCollection(value), type);
        }

        if (valueType.isMap()) {
            return convertMap(asMap(value), type);
        }

        Optional<Class<?>> basicTargetType = conversions.getCustomWriteTarget(value.getClass());
        return basicTargetType
            .<Object>map(aClass -> conversionService.convert(value, aClass))
            .orElseGet(() -> convertCustomType(value, valueType));
    }

    protected List<Object> convertCollection(final Collection<?> source, final TypeInformation<?> type) {
        Assert.notNull(source, "Given collection must not be null!");
        Assert.notNull(type, "Given type must not be null!");

        TypeInformation<?> componentType = type.getComponentType();

        return source.stream().map(element -> getValueToWrite(element, componentType)).collect(Collectors.toList());
    }

    protected Map<Object, Object> convertMap(final Map<Object, Object> source, final TypeInformation<?> type) {
        Assert.notNull(source, "Given map must not be null!");
        Assert.notNull(type, "Given type must not be null!");

        Supplier<Map<Object, Object>> mapSupplier;
        if (settings.isWriteTreeMaps()) {
            mapSupplier = TreeMap::new;
        } else {
            mapSupplier = HashMap::new;
        }
        return source.entrySet().stream().collect(mapSupplier, (m, e) -> {
            Object key = e.getKey();
            Object value = e.getValue();
            if (key == null && settings.isWriteTreeMaps()) {
                throw new UnsupportedOperationException("Key of a map cannot be null");
            }

            if (!conversions.isSimpleType(key.getClass())) {
                throw new MappingException("Cannot use a complex object as a map key");
            }

            Object simpleKey;

            if (settings.isKeepOriginalKeyTypes() &&
                isValidAerospikeMapKeyType(key.getClass())) {
                simpleKey = key;
            } else {
                if (conversionService.canConvert(key.getClass(), String.class)) {
                    simpleKey = conversionService.convert(key, String.class);
                } else {
                    simpleKey = key.toString();
                }
            }

            Object convertedValue = getValueToWrite(value, type.getMapValueType());
            if (simpleKey instanceof byte[]) simpleKey = ByteBuffer.wrap((byte[]) simpleKey);
            m.put(simpleKey, convertedValue);
        }, Map::putAll);
    }

    private Map<String, Object> convertCustomType(Object source, TypeInformation<?> type) {
        Assert.notNull(source, "Given map must not be null!");
        Assert.notNull(type, "Given type must not be null!");

        AerospikePersistentEntity<?> entity;
        try {
            entity = mappingContext.getRequiredPersistentEntity(source.getClass());
        } catch (Exception e) {
            throw new AerospikeException("Exception while getting persistent entity", e);
        }
        ConvertingPropertyAccessor<?> accessor =
            new ConvertingPropertyAccessor<>(entity.getPropertyAccessor(source), conversionService);

        return convertProperties(type, entity, accessor, true);
    }

    @SuppressWarnings("unchecked")
    private Map<Object, Object> asMap(Object value) {
        return (Map<Object, Object>) value;
    }

    private int getExpiration(AerospikePersistentEntity<?> entity, ConvertingPropertyAccessor<?> accessor) {
        AerospikePersistentProperty expirationProperty = entity.getExpirationProperty();
        if (expirationProperty != null) {
            return getExpirationFromProperty(accessor, expirationProperty);
        }

        return entity.getExpiration();
    }

    private int getExpirationFromProperty(ConvertingPropertyAccessor<?> accessor,
                                          AerospikePersistentProperty expirationProperty) {
        if (expirationProperty.isExpirationSpecifiedAsUnixTime()) {
            Long unixTime = accessor.getProperty(expirationProperty, Long.class);
            Assert.notNull(unixTime, "Expiration must not be null!");
            int inSeconds = unixTimeToOffsetInSeconds(unixTime);
            Assert.isTrue(inSeconds > 0, "Expiration value must be greater than zero, but was: "
                + inSeconds + " seconds (unix time: " + unixTime + ")");
            return inSeconds;
        }

        Integer expirationInSeconds = accessor.getProperty(expirationProperty, Integer.class);
        Assert.notNull(expirationInSeconds, "Expiration must not be null!");

        return expirationInSeconds;
    }

    private boolean isValidAerospikeRecordKeyType(Class<?> type) {
        return type == Short.TYPE || type == Integer.TYPE || type == Long.TYPE || type == Byte.TYPE ||
            type == Character.TYPE ||
            type == Short.class || type == Integer.class || type == Long.class || type == Byte.class ||
            type == Character.class || type == String.class ||
            type == byte[].class;
    }

    private Key constructAerospikeRecordKey(String namespace, String set, Object userKey) {
        // At this point there are no primitives types for userKey, only wrappers
        if (userKey.getClass() == String.class) {
            return new Key(namespace, set, (String) userKey);
        } else if (userKey.getClass() == Short.class) {
            return new Key(namespace, set, (short) userKey);
        } else if (userKey.getClass() == Integer.class) {
            return new Key(namespace, set, (int) userKey);
        } else if (userKey.getClass() == Long.class) {
            return new Key(namespace, set, (long) userKey);
        } else if (userKey.getClass() == Character.class) {
            return new Key(namespace, set, (char) userKey);
        } else if (userKey.getClass() == Byte.class) {
            return new Key(namespace, set, (byte) userKey);
        } else if (userKey.getClass() == byte[].class) {
            return new Key(namespace, set, (byte[]) userKey);
        }
        // Could not construct a key of native supported Aerospike record key type
        return null;
    }

    private boolean isValidAerospikeMapKeyType(Class<?> type) {
        return isValidAerospikeRecordKeyType(type) || type == Double.TYPE || type == Double.class;
    }
}
