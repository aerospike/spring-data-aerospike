package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.query.qualifier.QualifierBuilder;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.TreeMap;

import static org.springframework.data.aerospike.convert.AerospikeConverter.CLASS_KEY;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion.NULL_PARAM;
import static org.springframework.util.ClassUtils.isAssignable;
import static org.springframework.util.ClassUtils.isAssignableValue;

public class AerospikeQueryCreatorUtils {

    protected static Qualifier setQualifier(MappingAerospikeConverter converter, QualifierBuilder qb,
                                            String fieldName, FilterOperation op, Part part, List<String> dotPath) {
        qb.setField(fieldName)
            .setFilterOperation(op)
            .setIgnoreCase(ignoreCaseToBoolean(part))
            .setConverter(converter);
        if (dotPath != null && !qb.hasDotPath()) qb.setDotPath(dotPath);

        return qb.build();
    }

    protected static Object convertNullParameter(Object value) {
        return (value == NULL_PARAM) ? Value.getAsNull() : value;
    }

    protected static boolean ignoreCaseToBoolean(Part part) {
        return switch (part.shouldIgnoreCase()) {
            case WHEN_POSSIBLE -> part.getProperty().getType() == String.class;
            case ALWAYS -> true;
            default -> false;
        };
    }

    /**
     * Iterate over nested properties until the current one
     */
    protected static PropertyPath getNestedPropertyPath(PropertyPath propertyPath) {
        PropertyPath result = null;
        for (PropertyPath current = propertyPath; current != null; current = current.next()) {
            result = current;
        }
        return result;
    }

    protected static Class<?> getCollectionElementsClass(PropertyPath property) {
        // Get the class of object's elements
        // Expected to return non-null value as far as property is a Collection
        return property.getTypeInformation().getComponentType().getType();
    }

    protected static Qualifier qualifierAndConcatenated(MappingAerospikeConverter converter, List<Object> params,
                                                        QualifierBuilder qb,
                                                        Part part, String fieldName, FilterOperation op,
                                                        List<String> dotPath) {
        return qualifierAndConcatenated(converter, params, qb, part, fieldName, op, dotPath, false);
    }

    protected static Qualifier qualifierAndConcatenated(MappingAerospikeConverter converter, List<Object> params,
                                                        QualifierBuilder qb,
                                                        Part part, String fieldName, FilterOperation op,
                                                        List<String> dotPath, boolean containingMapKeyValuePairs) {
        Qualifier[] qualifiers;
        if (containingMapKeyValuePairs) {
            qualifiers = new Qualifier[params.size() / 2]; // keys/values qty must be even
            for (int i = 0, j = 0; i < params.size(); i += 2, j++) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i + 1));
                qualifiers[j] = setQualifier(converter, qb, fieldName, op, part, dotPath);
            }
        }
        qualifiers = new Qualifier[params.size()];
        for (int i = 0; i < params.size(); i++) {
            setQbValuesForMapByKey(qb, params.get(i), params.get(i));
            qualifiers[i] = setQualifier(converter, qb, fieldName, op, part, dotPath);
        }

        return Qualifier.and(qualifiers);
    }

    protected static String getFieldName(String segmentName, AerospikePersistentProperty property) {
        org.springframework.data.aerospike.mapping.Field annotation =
            property.findAnnotation(org.springframework.data.aerospike.mapping.Field.class);

        if (annotation != null && StringUtils.hasText(annotation.value())) {
            return annotation.value();
        }

        if (!StringUtils.hasText(segmentName)) {
            throw new IllegalStateException("Segment name is null or empty");
        }

        return segmentName;
    }

    protected static void setQbValuesForMapByKey(QualifierBuilder qb, Object key, Object value) {
        qb.setKey(Value.get(value)); // contains value
        qb.setValue(Value.get(key)); // contains key
    }

    protected static Object convertIfNecessary(Object obj, MappingAerospikeConverter converter) {
        if (obj == null || obj instanceof AerospikeQueryCriterion || obj instanceof AerospikeNullQueryCriterion) {
            return obj;
        }

        // converting if necessary (e.g., Date to Long so that proper filter expression or sIndex filter can be built)
        TypeInformation<?> valueType = TypeInformation.of(obj.getClass());
        return converter.toWritableValue(obj, valueType);
    }

    protected static Value getValueOfQueryParameter(Object queryParameter) {
        return Value.get(convertNullParameter(queryParameter));
    }

    protected static void setQualifierBuilderKey(QualifierBuilder qb, Object key) {
        qb.setKey(getValueOfQueryParameter(key));
    }

    protected static void setQualifierBuilderValue(QualifierBuilder qb, Object value) {
        qb.setValue(getValueOfQueryParameter(value));
    }

    protected static void setQualifierBuilderSecondValue(QualifierBuilder qb, Object value) {
        qb.setSecondValue(getValueOfQueryParameter(value));
    }

    protected static FilterOperation getCorrespondingMapValueFilterOperationOrFail(FilterOperation op) {
        try {
            return FilterOperation.valueOf("MAP_VAL_" + op + "_BY_KEY");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot find corresponding MAP_VAL_..._BY_KEY FilterOperation for '" + op + "'");
        }
    }

    protected static boolean isPojo(Class<?> clazz) { // if it is a first level POJO or a Map
        TypeInformation<?> type = TypeInformation.of(clazz);
        return !Utils.isSimpleValueType(clazz) && !type.isCollectionLike();
    }

    protected static void validateTypes(MappingAerospikeConverter converter, PropertyPath property, FilterOperation op,
                                        List<Object> queryParameters) {
        String queryPartDescription = String.join(" ", property.toString(), op.toString());
        validateTypes(converter, property, queryParameters, queryPartDescription);
    }

    protected static void validateTypes(MappingAerospikeConverter converter, PropertyPath property,
                                        List<Object> queryParameters, String queryPartDescription) {
        validateTypes(converter, property.getTypeInformation().getType(), queryParameters, queryPartDescription);
    }

    protected static void validateTypes(MappingAerospikeConverter converter, Class<?> propertyType,
                                        List<Object> queryParameters, String queryPartDescription,
                                        String... alternativeTypes) {
        // Checking versus Number rather than strict type to be able to compare, e.g., integer to a long
        if (isAssignable(Number.class, propertyType) && isAssignableValue(Number.class, queryParameters.get(0))) {
            propertyType = Number.class;
        }

        Class<?> clazz = propertyType;
        if (!queryParameters.stream().allMatch(param -> isAssignableValueOrConverted(clazz, param, converter))) {
            String validTypes = propertyType.getSimpleName();
            if (alternativeTypes.length > 0) {
                validTypes = String.format("one of the following types: %s", propertyType.getSimpleName() + ", "
                    + String.join(", ", alternativeTypes));
            }
            throw new IllegalArgumentException(String.format("%s: Type mismatch, expecting %s", queryPartDescription,
                validTypes));
        }
    }

    protected static boolean isAssignableValueOrConverted(Class<?> propertyType, Object obj,
                                                          MappingAerospikeConverter converter) {
        return isAssignableValue(propertyType, obj)
            || converter.getCustomConversions().hasCustomReadTarget(obj.getClass(), propertyType)
            || isPojoMap(obj, propertyType);
    }

    /**
     * Check if an object is a converted POJO with the same class as the given propertyType
     *
     * @param object       Instance to be compared
     * @param propertyType Class for comparing
     * @return Whether the object is a converted POJO of the given class
     */
    protected static boolean isPojoMap(Object object, Class<?> propertyType) {
        if (object instanceof TreeMap<?, ?> treeMap) {
            Object classKey = treeMap.get(CLASS_KEY);
            return classKey != null && classKey.equals(propertyType.getName());
        }
        return false;
    }
}
