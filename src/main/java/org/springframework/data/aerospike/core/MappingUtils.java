package org.springframework.data.aerospike.core;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.KeyRecord;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.Field;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.springframework.data.aerospike.util.Utils.iterableToList;

/**
 * A utility class providing methods for entity mapping and persistence operations.
 */
public class MappingUtils {

    private MappingUtils() {
        // Private constructor to prevent instantiation
        throw new UnsupportedOperationException("Utility class MappingUtils cannot be instantiated");
    }

    static <T> T mapToEntity(KeyRecord keyRecord, Class<T> targetClass, MappingAerospikeConverter converter) {
        return mapToEntity(keyRecord.key, targetClass, keyRecord.record, converter);
    }

    static <T> T mapToEntity(Key key, Class<T> clazz, Record aeroRecord, MappingAerospikeConverter converter) {
        if (aeroRecord == null) {
            return null;
        }
        AerospikeReadData data = AerospikeReadData.forRead(key, aeroRecord);
        return converter.read(clazz, data);
    }

    static GroupedEntities toGroupedEntities(EntitiesKeys entitiesKeys, Record[] records,
                                             MappingAerospikeConverter converter) {
        GroupedEntities.GroupedEntitiesBuilder builder = GroupedEntities.builder();

        IntStream.range(0, entitiesKeys.getKeys().length)
            .filter(index -> records[index] != null)
            .mapToObj(index -> mapToEntity(entitiesKeys.getKeys()[index], entitiesKeys.getEntityClasses()[index],
                records[index], converter))
            .filter(Objects::nonNull)
            .forEach(entity -> builder.entity(getEntityClass(entity), entity));

        return builder.build();
    }

    static Map<Class<?>, List<Key>> toEntitiesKeyMap(GroupedKeys groupedKeys, TemplateContext templateContext) {
        return groupedKeys.getEntitiesKeys().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry ->
                toKeysList(entry.getKey(), entry.getValue(), templateContext)));
    }

    static <T> List<String> fieldsToBinNames(T document, Collection<String> fields, TemplateContext templateContext) {
        AerospikePersistentEntity<?> entity =
            templateContext.mappingContext.getRequiredPersistentEntity(document.getClass());

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
                throw ExceptionUtils.translateError(new AerospikeException("Cannot convert field: " + field +
                    " to bin name. field doesn't exists."), templateContext.exceptionTranslator);
            })
            .collect(Collectors.toList());
    }

    private static String getFieldAnnotatedValue(AerospikePersistentEntity<?> entity, String field) {
        for (AerospikePersistentProperty property : entity.getPersistentProperties(Field.class)) {
            if (property.getFieldName().equals(field)) {
                return field;
            }
        }
        return null;
    }

    private static <T> List<Key> toKeysList(Class<T> entityClass, Collection<?> ids, TemplateContext templateContext) {
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(ids, "List of ids must not be null!");

        AerospikePersistentEntity<?> entity = templateContext.mappingContext.getRequiredPersistentEntity(entityClass);
        List<?> idsList = iterableToList(ids);

        return idsList.stream()
            .map(id -> TemplateUtils.getKey(id, entity, templateContext))
            .collect(Collectors.toList());
    }

    static Calendar convertToCalendar(Instant instant) {
        if (instant == null) return null;

        Calendar calendar = Calendar.getInstance();
        if (instant.toEpochMilli() > calendar.getTimeInMillis())
            throw new IllegalArgumentException("Last update time (%d) must be less than the current time"
                .formatted(instant.toEpochMilli()));
        calendar.setTime(Date.from(instant));
        return calendar;
    }

    @SuppressWarnings({"unchecked", "SameParameterValue"})
    static <S> S convertIfNecessary(MappingAerospikeConverter converter, Object source, Class<S> type) {
        return type.isAssignableFrom(source.getClass()) ? (S) source
            : converter.getConversionService().convert(source, type);
    }

    static <T, S> Class<?> getTargetClass(Class<T> entityClass, Class<S> targetClass) {
        if (targetClass != null && targetClass != entityClass) {
            return targetClass;
        }
        return entityClass;
    }

    static String[] getBinNamesFromTargetClassOrNull(Class<?> entityClass, @Nullable Class<?> targetClass,
                                                     MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext) {
        if (targetClass == null || targetClass == entityClass) {
            return null;
        }
        return getBinNamesFromTargetClass(targetClass, mappingContext);
    }

    public static String[] getBinNamesFromTargetClass(Class<?> targetClass,
                                                      MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext) {
        AerospikePersistentEntity<?> targetEntity = mappingContext.getRequiredPersistentEntity(targetClass);

        List<String> binNamesList = new ArrayList<>();

        targetEntity.doWithProperties(
            (PropertyHandler<AerospikePersistentProperty>) property -> {
                if (!property.isIdProperty()) {
                    binNamesList.add(property.getFieldName());
                }
            });

        return binNamesList.toArray(new String[0]);
    }

    @SuppressWarnings("unchecked")
    static <T> Class<T> getEntityClass(T entity) {
        return (Class<T>) entity.getClass();
    }
}
