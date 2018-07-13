package org.springframework.data.aerospike.convert;

import static org.springframework.data.aerospike.convert.AerospikeMetaData.USER_KEY;
import static org.springframework.data.aerospike.utility.TimeUtils.offsetInSecondsToUnixTime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.springframework.core.CollectionFactory;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.PreferredConstructor;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;

public class MappingAerospikeReadConverter implements EntityReader<Object, AerospikeReadData> {

	private final EntityInstantiators entityInstantiators;
	private final TypeMapper<Map<String, Object>> typeMapper;
	private final AerospikeMappingContext mappingContext;
	private final CustomConversions conversions;
	private final GenericConversionService conversionService;

	public MappingAerospikeReadConverter(EntityInstantiators entityInstantiators, TypeMapper<Map<String, Object>> typeMapper,
										 AerospikeMappingContext mappingContext, CustomConversions conversions,
										 GenericConversionService conversionService) {
		this.entityInstantiators = entityInstantiators;
		this.typeMapper = typeMapper;
		this.mappingContext = mappingContext;
		this.conversions = conversions;
		this.conversionService = conversionService;
	}

	/*
	* (non-Javadoc)
	* @see org.springframework.data.convert.EntityReader#read(java.lang.Class, S)
	*/
	@Override
	public <R> R read(Class<R> targetClass, final AerospikeReadData data) {
		if (data == null) {
			return null;
		}

		Map<String, Object> record = data.getRecord();
		TypeInformation<? extends R> typeToUse = typeMapper.readType(record, ClassTypeInformation.from(targetClass));
		Class<? extends R> rawType = typeToUse.getType();
		if (conversions.hasCustomReadTarget(AerospikeReadData.class, rawType)) {
			return conversionService.convert(data, rawType);
		}

		AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);
		RecordReadingPropertyValueProvider propertyValueProvider = new RecordReadingPropertyValueProvider(data.getKey(), record);
		ConvertingPropertyAccessor accessor = getConvertingPropertyAccessor(entity, propertyValueProvider);

		AerospikePersistentProperty idProperty = entity.getIdProperty();
		if (idProperty != null) {
			Object value = getIdValue(data.getKey(), record, idProperty);
			accessor.setProperty(idProperty, value);
		}
		AerospikePersistentProperty expirationProperty = entity.getExpirationProperty();
		if (expirationProperty != null) {
			Object value = getExpiration(data, expirationProperty);
			accessor.setProperty(expirationProperty, value);
		}

		return convertProperties(entity, propertyValueProvider, accessor);
	}

	@SuppressWarnings("unchecked")
	private <T> T getIdValue(Key key, Map<String, Object> data, AerospikePersistentProperty property) {
		Value userKey = key.userKey;
		Object value = userKey == null ? data.get(USER_KEY) : userKey.getObject();
		Assert.notNull(value, "Id must not be null!");
		return (T) convertIfNeeded(value, property.getType());
	}

	private <R> R convertProperties(AerospikePersistentEntity<?> entity,
									RecordReadingPropertyValueProvider propertyValueProvider,
									PersistentPropertyAccessor accessor) {
		entity.doWithProperties((PropertyHandler<AerospikePersistentProperty>) persistentProperty -> {

			PreferredConstructor<?, AerospikePersistentProperty> constructor = entity.getPersistenceConstructor();

			if (isNotReadable(constructor, persistentProperty)) {
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

	private boolean isNotReadable(PreferredConstructor<?, AerospikePersistentProperty> constructor,
								  AerospikePersistentProperty property) {
		return constructor.isConstructorParameter(property) || property.isIdProperty() || property.isExpirationProperty();
	}

	private <T> T readValue(Object source, TypeInformation<?> propertyType) {
		Assert.notNull(propertyType, "Target type must not be null!");

		if (source == null) {
			return null;
		}
		Class<?> targetClass = propertyType.getType();
		if (conversions.hasCustomReadTarget(source.getClass(), targetClass)) {
			return (T) conversionService.convert(source, targetClass);
		} else if (propertyType.isCollectionLike()) {
			return convertCollection((Collection) source, propertyType);
		} else if (propertyType.isMap()) {
			return (T) convertMap((Map<String, Object>) source, propertyType);
		} else if (source instanceof Map) { // custom type
			return convertCustomType((Map<String, Object>) source, propertyType);
		}
		return (T) convertIfNeeded(source, targetClass);
	}

	private <T> T convertCustomType(Map<String, Object> source, TypeInformation<?> propertyType) {
		TypeInformation<?> typeToUse = typeMapper.readType(source, propertyType);
		AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(typeToUse);
		RecordReadingPropertyValueProvider propertyValueProvider = new RecordReadingPropertyValueProvider(source);
		PersistentPropertyAccessor persistentPropertyAccessor = getConvertingPropertyAccessor(entity, propertyValueProvider);
		return (T) convertProperties(entity, propertyValueProvider, persistentPropertyAccessor);
	}

	private <R> R convertMap(Map<String, Object> source, TypeInformation<?> propertyType) {
		Class<?> mapClass = propertyType.getType();
		TypeInformation<?> keyType = propertyType.getComponentType();
		Class<?> keyClass = keyType == null ? null : keyType.getType();
		TypeInformation<?> mapValueType = propertyType.getMapValueType();

		Map<Object, Object> converted = CollectionFactory.createMap(mapClass, keyClass, source.keySet().size());

		source.entrySet()
				.forEach((e) -> {
					Object key = (keyClass != null) ? conversionService.convert(e.getKey(), keyClass) : e.getKey();
					Object value = readValue(e.getValue(), mapValueType);
					converted.put(key, value);
				});

		return (R) convertIfNeeded(converted, propertyType.getType());
	}

	private <R> R convertCollection(final Collection source, final TypeInformation<?> propertyType) {
		Class<?> collectionClass = propertyType.getType();
		TypeInformation<?> elementType = propertyType.getComponentType();
		Class<?> elementClass = elementType == null ? null : elementType.getType();

		Collection<Object> items = collectionClass.isArray() ? new ArrayList<>() :
				CollectionFactory.createCollection(collectionClass, elementClass, source.size());

		source.forEach(item -> items.add(readValue(item, elementType)));

		return (R) convertIfNeeded(items, propertyType.getType());
	}

	private Object convertIfNeeded(Object value, Class<?> targetClass) {
		if (Enum.class.isAssignableFrom(targetClass)) {
			return Enum.valueOf((Class<Enum>) targetClass, value.toString());
		}
		return targetClass.isAssignableFrom(value.getClass()) ? value : conversionService.convert(value, targetClass);
	}

	private ConvertingPropertyAccessor getConvertingPropertyAccessor(AerospikePersistentEntity<?> entity,
																	 RecordReadingPropertyValueProvider recordReadingPropertyValueProvider) {
		EntityInstantiator instantiator = entityInstantiators.getInstantiatorFor(entity);
		Object instance = instantiator.createInstance(entity, new PersistentEntityParameterValueProvider<>(entity,
				recordReadingPropertyValueProvider, null));

		return new ConvertingPropertyAccessor(entity.getPropertyAccessor(instance), conversionService);
	}

	private Object getExpiration(AerospikeReadData data, AerospikePersistentProperty expirationProperty) {
		if (expirationProperty.isExpirationSpecifiedAsUnixTime()) {
			return offsetInSecondsToUnixTime(data.getExpiration());
		}

		return data.getExpiration();
	}

	/**
	 * A {@link PropertyValueProvider} to lookup values on the configured {@link Record}.
	 *
	 * @author Oliver Gierke
	 */
	private class RecordReadingPropertyValueProvider implements PropertyValueProvider<AerospikePersistentProperty> {

		private final Key key;
		private final Map<String, Object> source;

		public RecordReadingPropertyValueProvider(Key key, Map<String, Object> source) {
			this.key = key;
			this.source = source;
		}

		public RecordReadingPropertyValueProvider(Map<String, Object> source) {
			this(null, source);
		}

		@Override
		public <T> T getPropertyValue(AerospikePersistentProperty property) {
			if (key != null && property.isIdProperty()) {
				return getIdValue(key, source, property);
			}
			Object value = source.get(property.getFieldName());

			return readValue(value, property.getTypeInformation());
		}

	}
}
