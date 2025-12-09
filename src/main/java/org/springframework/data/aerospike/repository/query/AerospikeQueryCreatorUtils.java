package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.dsl.ExpressionContext;
import com.aerospike.dsl.IndexContext;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.PlaceholderValues;
import com.aerospike.dsl.api.DSLParser;
import com.aerospike.dsl.client.query.IndexType;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.index.AerospikeContextDslResolverUtils;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.Utils;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.NonNull;
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.function.Predicate.not;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion.NULL_PARAM;
import static org.springframework.util.ClassUtils.isAssignable;
import static org.springframework.util.ClassUtils.isAssignableValue;

public class AerospikeQueryCreatorUtils {

    protected static Qualifier setQualifier(QueryQualifierBuilder qb, String binName, FilterOperation op, Part part,
                                            List<String> dotPath, ServerVersionSupport versionSupport) {
        qb.setBinName(binName)
            .setInnerQbFilterOperation(op)
            .setIgnoreCase(ignoreCaseToBoolean(part));
        if (dotPath != null && !qb.hasDotPath()) {
            qb.setDotPath(dotPath);
            String[] dotPathArr = getDotPathArray(dotPath);
            if (dotPathArr != null && dotPathArr.length > 2) {
                List<String> ctxList = convertToStringListExclStartAndEnd(dotPathArr);
                qb.setCtxArray(resolveCtxList(ctxList));
            }
        }
        qb.setServerVersionSupport(versionSupport);
        return qb.build();
    }

    public static CTX[] resolveCtxList(List<String> ctxList) {
        return ctxList.stream()
            .filter(not(String::isEmpty))
            .map(AerospikeContextDslResolverUtils::toCtx)
            .filter(Objects::nonNull)
            .toArray(CTX[]::new);
    }

    public static String[] getDotPathArray(List<String> dotPathList) {
        if (dotPathList != null && !dotPathList.isEmpty()) {
            // the first element of dotPath is part.getProperty().toDotPath()
            // the second element of dotPath, if present, is a value
            Stream<String> valueStream = dotPathList.size() == 1 || dotPathList.get(1) == null ? Stream.empty()
                : Stream.of(dotPathList.get(1));
            return Stream.concat(Arrays.stream(dotPathList.get(0).split("\\.")), valueStream)
                .toArray(String[]::new);
        }
        return null;
    }

    /**
     * Convert a String array into String List excluding the first and the last elements
     *
     * @param array String array
     * @return String List
     */
    protected static List<String> convertToStringListExclStartAndEnd(@NonNull String[] array) {
        return Arrays.stream(array)
            .skip(1) // first element is bin name
            .limit(array.length - 2L) // last element is the key we already have
            .collect(Collectors.toList());
    }

    /**
     * Convert a String array into String List excluding the first element
     *
     * @param array String array
     * @return String List
     */
    public static List<String> convertToStringListExclStart(@NonNull String[] array) {
        return Arrays.stream(array)
            .skip(1) // first element is bin name
            .limit(array.length - 1L)
            .collect(Collectors.toList());
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
        if (property.getTypeInformation().getComponentType() == null) {
            return null;
        }
        return property.getTypeInformation().getComponentType().getType();
    }

    protected static Qualifier qualifierAndConcatenated(ServerVersionSupport versionSupport, List<Object> params,
                                                        QueryQualifierBuilder qb,
                                                        Part part, String fieldName, FilterOperation op,
                                                        List<String> dotPath) {
        return qualifierAndConcatenated(versionSupport, params, qb, part, fieldName, op, dotPath, false);
    }

    protected static Qualifier qualifierAndConcatenated(ServerVersionSupport versionSupport, List<Object> params,
                                                        QueryQualifierBuilder qb,
                                                        Part part, String fieldName, FilterOperation op,
                                                        List<String> dotPath, boolean containingMapKeyValuePairs) {
        Qualifier[] qualifiers;
        if (containingMapKeyValuePairs) {
            qualifiers = new Qualifier[params.size() / 2]; // keys/values qty must be even
            for (int i = 0, j = 0; i < params.size(); i += 2, j++) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i + 1));
                qualifiers[j] = setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
            }
        }
        qualifiers = new Qualifier[params.size()];
        for (int i = 0; i < params.size(); i++) {
            setQbValuesForMapByKey(qb, params.get(i), params.get(i));
            qualifiers[i] = setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
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

    protected static void setQbValuesForMapByKey(QueryQualifierBuilder qb, Object key, Object value) {
        qb.setKey(Value.get(value)); // contains value
        qb.setValue(key); // contains key
    }

    protected static Object convertIfNecessary(Object obj, MappingAerospikeConverter converter) {
        if (typeDoesNotRequireConversion(obj)) {
            return obj;
        }

        // converting if necessary (e.g., Date to Long so that proper filter expression or sIndex filter can be built)
        TypeInformation<?> valueType = TypeInformation.of(obj.getClass());
        return converter.toWritableValue(obj, valueType);
    }

    private static boolean typeDoesNotRequireConversion(Object obj) {
        return obj == null
            || obj instanceof AerospikeQueryCriterion
            || obj instanceof AerospikeNullQueryCriterion;
    }

    protected static Value getValueOfQueryParameter(Object queryParameter) {
        return Value.get(convertNullParameter(queryParameter));
    }

    protected static void setQualifierBuilderKey(QueryQualifierBuilder qb, Object key) {
        qb.setKey(getValueOfQueryParameter(key));
    }

    protected static void setQualifierBuilderSecondKey(QueryQualifierBuilder qb, Object key) {
        qb.setNestedKey(getValueOfQueryParameter(key));
    }

    protected static void setQualifierBuilderValue(QueryQualifierBuilder qb, Object value) {
        qb.setValue(getValueOfQueryParameter(value));
    }

    protected static void setQualifierBuilderSecondValue(QueryQualifierBuilder qb, Object value) {
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

    protected static void validateTypes(MappingAerospikeConverter converter, PropertyPath propertyPath,
                                        FilterOperation op, List<Object> queryParameters) {
        String queryPartDescription = String.join(" ", propertyPath.toString(), op.toString());
        validateTypes(converter, propertyPath, queryParameters, op, queryPartDescription);
    }

    protected static void validateTypes(MappingAerospikeConverter converter, PropertyPath propertyPath,
                                        List<Object> queryParameters, FilterOperation op, String queryPartDescription) {
        validateTypes(converter, propertyPath.getTypeInformation().getType(), queryParameters, op,
            queryPartDescription);
    }

    protected static void validateTypes(MappingAerospikeConverter converter, Class<?> propertyType,
                                        List<Object> queryParameters, FilterOperation op, String queryPartDescription,
                                        String... alternativeTypes) {
        // Checking versus Number rather than strict type to be able to compare, e.g., integer to a long
        if (propertyTypeAndFirstParamAssignableToNumber(propertyType, queryParameters)) {
            propertyType = Number.class;
        }

        Class<?> clazz = propertyType;
        Stream<Object> params = queryParameters.stream();
        if ((op == FilterOperation.IN || op == FilterOperation.NOT_IN)
            && queryParameters.size() == 1
            && queryParameters.get(0) instanceof Collection<?>) {
            //noinspection unchecked
            params = ((Collection<Object>) queryParameters.get(0)).stream();
        }

        // skipping further validations as they depend on using classKey
        if (!StringUtils.hasText(converter.getAerospikeDataSettings().getClassKey())) return;

        if (!params.allMatch(param -> isAssignableValueOrConverted(clazz, param, converter))) {
            String validTypes = propertyType.getSimpleName();
            if (alternativeTypes.length > 0) {
                validTypes = String.format("one of the following types: %s", propertyType.getSimpleName() + ", "
                    + String.join(", ", alternativeTypes));
            }
            throw new IllegalArgumentException(String.format("%s: Type mismatch, expecting %s", queryPartDescription,
                validTypes));
        }
    }

    private static boolean propertyTypeAndFirstParamAssignableToNumber(Class<?> propertyType,
                                                                       List<Object> queryParameters) {
        return !queryParameters.isEmpty()
            && isAssignable(Number.class, propertyType)
            && isAssignableValue(Number.class, queryParameters.get(0));
    }

    protected static void validateQueryIsNull(List<Object> queryParameters, String queryPartDescription) {
        // Number of arguments is not zero
        if (!queryParameters.isEmpty()) {
            throw new IllegalArgumentException(queryPartDescription + ": expecting no arguments");
        }
    }

    protected static void validateQueryIn(List<Object> queryParameters, String queryPartDescription) {
        // Number of arguments is not one
        if (queryParameters.size() != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }
    }

    // works only when classKey configuration property is not null
    protected static boolean isAssignableValueOrConverted(Class<?> propertyType, Object obj,
                                                          MappingAerospikeConverter converter) {
        return isAssignableValue(propertyType, obj)
            || converter.getCustomConversions().hasCustomReadTarget(obj.getClass(), propertyType)
            // POJOs and enums got converted to Strings when query parameters were set
            || isPojoMap(obj, propertyType, converter.getAerospikeDataSettings().getClassKey())
            || (propertyType.isEnum() && obj instanceof String);
    }

    /**
     * Check if an object is a converted POJO with the same class as the given propertyType
     *
     * @param object       Instance to be compared
     * @param propertyType Class for comparing
     * @param classKey     Name of the bin to store entity's class
     * @return Whether the object is a converted POJO of the given class
     */
    protected static boolean isPojoMap(Object object, Class<?> propertyType, String classKey) {
        if (object instanceof TreeMap<?, ?> treeMap) {
            Object className = treeMap.get(classKey);
            return className != null && className.equals(propertyType.getName());
        }
        return false;
    }

    private static boolean areNamesEqual(com.aerospike.dsl.Index idx, String indexToUse) {
        return idx != null && idx.getName() != null && idx.getName().equals(indexToUse);
    }

    /**
     * Parse DSL expression by providing DSL string, existing indexes and placeholder values to {@link DSLParser}
     *
     * @param dslExpression             DSL string to use
     * @param namespace                 Namespace to use
     * @param indexToUse                Explicitly given secondary index name
     * @param indexCache                Cache of existing secondary indexes
     * @param parameters                Values to replace DSL expression placeholders with
     * @param dslParser                 {@link DSLParser} instance
     * @return {@link ParsedExpression}
     */
    public static ParsedExpression parseDslExpression(String dslExpression, String namespace,
                                                      String indexToUse,
                                                      Map<IndexKey, Index> indexCache, Object[] parameters,
                                                      DSLParser dslParser) {
        // Map cached indexes to DSLParser input
        List<com.aerospike.dsl.Index> indexes = indexCache.values().stream().map(value ->
                com.aerospike.dsl.Index.builder()
                    .name(value.getName())
                    .namespace(value.getNamespace())
                    .bin(value.getBin())
                    .indexType(value.getIndexType() == null ? null : IndexType.valueOf(value.getIndexType().name()))
                    .binValuesRatio(value.getBinValuesRatio())
                    .build())
            .toList();

        // Use explicitly given secondary index if it is provided
        List<com.aerospike.dsl.Index> singleIndexList = indexes.stream().filter(idx -> areNamesEqual(idx, indexToUse)).toList();

        // Parse the given expression
        return dslParser.parseExpression(
            ExpressionContext.of(dslExpression, PlaceholderValues.of(parameters)),
            indexes.isEmpty() ? null : IndexContext.of(namespace, singleIndexList.isEmpty() ? indexes : singleIndexList)
        );
    }
}
