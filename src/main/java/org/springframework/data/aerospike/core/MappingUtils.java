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

    /**
     * Maps a {@link KeyRecord} to an entity of the specified target class. This method extracts the key and record from
     * the KeyRecord and delegates to {@link #mapToEntity(Key, Class, Record, MappingAerospikeConverter)}.
     *
     * @param <T>         The type of the target entity
     * @param keyRecord   The {@link KeyRecord} containing the Aerospike key and record
     * @param targetClass The class to which the record should be mapped
     * @param converter   The {@link MappingAerospikeConverter} used for mapping
     * @return An instance of the target class populated with data from the record, or {@code null} if the record is
     * null
     */
    static <T> T mapToEntity(KeyRecord keyRecord, Class<T> targetClass, MappingAerospikeConverter converter) {
        return mapToEntity(keyRecord.key, targetClass, keyRecord.record, converter);
    }

    /**
     * Maps a {@link Key} and {@link Record} to an entity of the specified class. If the record is null, this method
     * returns null. Otherwise, it uses the provided converter to read the data into an instance of the target class.
     *
     * @param <T>        The type of the entity
     * @param key        The {@link Key} associated with the record
     * @param clazz      The class to which the record should be mapped
     * @param aeroRecord The {@link Record} containing the data
     * @param converter  The {@link MappingAerospikeConverter} used for mapping
     * @return An instance of the specified class populated with data from the record, or {@code null} if the record is
     * null
     */
    static <T> T mapToEntity(Key key, Class<T> clazz, Record aeroRecord, MappingAerospikeConverter converter) {
        if (aeroRecord == null) {
            return null;
        }
        AerospikeReadData data = AerospikeReadData.forRead(key, aeroRecord);
        return converter.read(clazz, data);
    }

    /**
     * Converts an array of {@link Record}s and their corresponding {@link EntitiesKeys} into a {@link GroupedEntities}
     * object. This method iterates through the records, maps non-null records to their respective entity classes, and
     * then groups them by their entity class.
     *
     * @param entitiesKeys An {@link EntitiesKeys} object containing the keys and entity classes
     * @param records      An array of {@link Record}s
     * @param converter    The {@link MappingAerospikeConverter} used for mapping records to entities
     * @return A {@link GroupedEntities} object containing the mapped and grouped entities
     */
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

    /**
     * Converts a {@link GroupedKeys} object into a {@link Map} where keys are entity classes and values are lists of
     * {@link Key} objects. This facilitates operations that require keys grouped by their corresponding entity type.
     *
     * @param groupedKeys     The {@link GroupedKeys} object to convert
     * @param templateContext The context containing the template utilities for key generation
     * @return A {@link Map} of entity classes to lists of Aerospike client keys
     */
    static Map<Class<?>, List<Key>> toEntitiesKeyMap(GroupedKeys groupedKeys, TemplateContext templateContext) {
        return groupedKeys.getEntitiesKeys().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, entry ->
                toKeysList(entry.getKey(), entry.getValue(), templateContext)));
    }

    /**
     * Converts a collection of field names for a given document into their corresponding Aerospike bin names. This
     * method checks if a field is a class member or an already annotated bin name.
     *
     * @param <T>             The type of the document
     * @param document        The document instance
     * @param fields          A collection of field names to convert
     * @param templateContext The context containing the mapping context and exception translator
     * @return A {@link List} of strings representing the corresponding Aerospike bin names
     * @throws AerospikeException if a field cannot be converted to a bin name (e.g., field does not exist)
     */
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

    /**
     * Retrieves the value of a field annotated with {@code @Field} from an Aerospike persistent entity. This method
     * iterates through the persistent properties of the entity and checks if any property's field name matches the
     * given field and is annotated with {@code @Field}.
     *
     * @param entity The {@link AerospikePersistentEntity} to inspect
     * @param field  The field name to search for
     * @return The field name if found and annotated, otherwise {@code null}
     */
    private static String getFieldAnnotatedValue(AerospikePersistentEntity<?> entity, String field) {
        for (AerospikePersistentProperty property : entity.getPersistentProperties(Field.class)) {
            if (property.getFieldName().equals(field)) {
                return field;
            }
        }
        return null;
    }

    /**
     * Converts a collection of IDs for a specific class into a {@link List} of Aerospike client {@link Key} objects.
     *
     * @param <T>             The type of the entity
     * @param entityClass     The class of the entity for which the IDs belong
     * @param ids             A collection of IDs
     * @param templateContext The context containing the mapping context and template dependencies for key generation
     * @return A {@link List} of Aerospike client {@link Key} objects
     * @throws IllegalArgumentException if entity class or IDs list is null
     */
    private static <T> List<Key> toKeysList(Class<T> entityClass, Collection<?> ids, TemplateContext templateContext) {
        Assert.notNull(entityClass, "Entity class must not be null!");
        Assert.notNull(ids, "List of ids must not be null!");

        AerospikePersistentEntity<?> entity = templateContext.mappingContext.getRequiredPersistentEntity(entityClass);
        List<?> idsList = iterableToList(ids);

        return idsList.stream()
            .map(id -> TemplateUtils.getKey(id, entity, templateContext))
            .collect(Collectors.toList());
    }

    /**
     * Converts an {@link Instant} object to a {@link Calendar} instance. This method also includes a validation to
     * ensure the instant's epoch milliseconds are not greater than the current time's milliseconds.
     *
     * @param instant The {@link Instant} to convert
     * @return A {@link Calendar} instance representing the given instant, or {@code null} if the instant is null
     * @throws IllegalArgumentException if the instant's epoch milliseconds are greater than the current time
     */
    static Calendar convertToCalendar(Instant instant) {
        if (instant == null) return null;

        Calendar calendar = Calendar.getInstance();
        if (instant.toEpochMilli() > calendar.getTimeInMillis())
            throw new IllegalArgumentException("Last update time (%d) must be less than the current time"
                .formatted(instant.toEpochMilli()));
        calendar.setTime(Date.from(instant));
        return calendar;
    }

    /**
     * Converts a source object to a specified target type if necessary. If the source object is already assignable to
     * the target type, it is cast directly. Otherwise, the conversion service from the provided converter is used to
     * perform the conversion.
     *
     * @param <S>       The target type
     * @param converter The {@link MappingAerospikeConverter} providing the conversion service
     * @param source    The source object to convert
     * @param type      The target class to convert to
     * @return The converted object of the specified type
     */
    @SuppressWarnings({"unchecked", "SameParameterValue"})
    static <S> S convertIfNecessary(MappingAerospikeConverter converter, Object source, Class<S> type) {
        return type.isAssignableFrom(source.getClass()) ? (S) source
            : converter.getConversionService().convert(source, type);
    }

    /**
     * Determines the target class to be used for mapping or persistence operations. If a specific target class is
     * provided, and it differs from the entity class, the target class is returned. Otherwise, the entity class itself
     * is used.
     *
     * @param entityClass The primary class of the entity
     * @param targetClass An optional alternative target class
     * @param <T>         The type of the entity
     * @param <S>         The type of the target
     * @return The determined target class, which is either {@code targetClass} if specified and different or
     * {@code entityClass}
     */
    static <T, S> Class<?> getTargetClass(Class<T> entityClass, Class<S> targetClass) {
        if (targetClass != null && targetClass != entityClass) {
            return targetClass;
        }
        return entityClass;
    }

    /**
     * Retrieves an array of bin names from target class, if it is specified and differs from the given entity class,
     * otherwise returns {@code null}.
     *
     * @param entityClass    The primary class of the entity. Can be {@code null}
     * @param targetClass    An optional alternative target class. Can be {@code null}
     * @param mappingContext The mapping context used to retrieve persistent entity information
     * @return An array of bin names from the target class, or {@code null} if no distinct target class is used
     */
    public static String[] getBinNamesFromTargetClassOrNull(@Nullable Class<?> entityClass,
                                                            @Nullable Class<?> targetClass,
                                                            MappingContext<BasicAerospikePersistentEntity<?>,
                                                                AerospikePersistentProperty> mappingContext) {
        if (targetClass == null || targetClass == entityClass) {
            return null;
        }
        return getBinNamesFromTargetClass(targetClass, mappingContext);
    }

    /**
     * Retrieves an array of bin names for a given target class based on its persistent properties. This method iterates
     * through the properties of the target entity and collects their field names, excluding the ID property.
     *
     * @param targetClass    The class for which to retrieve bin names
     * @param mappingContext The mapping context used to retrieve persistent entity information
     * @return An array of strings representing the bin names associated with the target class's properties
     */
    public static String[] getBinNamesFromTargetClass(Class<?> targetClass,
                                                      MappingContext<BasicAerospikePersistentEntity<?>,
                                                          AerospikePersistentProperty> mappingContext) {
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

    /**
     * Retrieves the actual {@code Class} object for a given entity instance. This method performs an unchecked type
     * cast to ensure the returned class matches the generic type of the entity.
     *
     * @param <T>    The type of the entity
     * @param entity The instance of the entity
     * @return The {@code Class} object representing the runtime class of the entity
     */
    @SuppressWarnings("unchecked")
    static <T> Class<T> getEntityClass(T entity) {
        return (Class<T>) entity.getClass();
    }

    /**
     * Converts a collection of IDs into an array of Aerospike {@link Key} objects. Each ID is mapped to a Key using the
     * provided set name and template context.
     *
     * @param ids             A collection of IDs
     * @param setName         The name of the set associated with the keys
     * @param templateContext The context containing template dependencies for key generation
     * @return An array of Aerospike {@link Key} objects
     */
    static Key[] getKeys(Collection<?> ids, String setName, TemplateContext templateContext) {
        return ids.stream()
            .map(id -> TemplateUtils.getKey(id, setName, templateContext))
            .toArray(Key[]::new);
    }
}
