/*
 * Copyright 2012-2018 the original author or authors
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
package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriteria;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.StringUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.query.FilterOperation.*;
import static org.springframework.data.aerospike.query.Qualifier.idEquals;
import static org.springframework.data.aerospike.query.Qualifier.idIn;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriteria;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriteria.NULL;
import static org.springframework.data.aerospike.utility.Utils.isBoolean;
import static org.springframework.data.aerospike.utility.Utils.isSimpleValueType;
import static org.springframework.data.repository.query.parser.Part.Type.BETWEEN;
import static org.springframework.data.repository.query.parser.Part.Type.WITHIN;
import static org.springframework.util.ClassUtils.isAssignableValue;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreator extends AbstractQueryCreator<Query, CriteriaDefinition> {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeQueryCreator.class);
    private final AerospikeMappingContext context;
    private final MappingAerospikeConverter converter;
    private final boolean isCombinedQuery;

    public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters,
                                 AerospikeMappingContext context, MappingAerospikeConverter converter) {
        super(tree, parameters);
        this.context = context;
        this.converter = converter;
        this.isCombinedQuery = tree.getParts().toList().size() > 1;
    }

    @Override
    protected CriteriaDefinition create(Part part, Iterator<Object> iterator) {
        PersistentPropertyPath<AerospikePersistentProperty> path =
            context.getPersistentPropertyPath(part.getProperty());
        AerospikePersistentProperty property = path.getLeafProperty();
        Iterator<Object> paramIterator = iterator;

        if (isCombinedQuery && iterator.hasNext()) {
            Object nextParam = iterator.next();
            if (!(nextParam instanceof QueryParam)) {
                throw new IllegalArgumentException(String.format("%s: expected CombinedQueryParam, instead got %s",
                    part.getProperty(), nextParam.getClass().getSimpleName()));
            }
            paramIterator = Arrays.stream(((QueryParam) nextParam).arguments()).iterator();
        }

        return create(part, property, paramIterator);
    }

    private CriteriaDefinition create(Part part, AerospikePersistentProperty property, Iterator<?> parameters) {
        Object value1 = null;
        if (parameters.hasNext()) {
            value1 = parameters.next();
        }
        value1 = convertIfNecessary(value1);

        return switch (part.getType()) {
            case AFTER, GREATER_THAN -> getCriteria(part, property, value1, parameters, FilterOperation.GT);
            case GREATER_THAN_EQUAL -> getCriteria(part, property, value1, parameters, FilterOperation.GTEQ);
            case BEFORE, LESS_THAN -> getCriteria(part, property, value1, parameters, FilterOperation.LT);
            case LESS_THAN_EQUAL -> getCriteria(part, property, value1, parameters, FilterOperation.LTEQ);
            case BETWEEN -> getCriteria(part, property, value1, parameters, FilterOperation.BETWEEN);
            case LIKE, REGEX -> getCriteria(part, property, value1, parameters, FilterOperation.LIKE);
            case STARTING_WITH -> getCriteria(part, property, value1, parameters, FilterOperation.STARTS_WITH);
            case ENDING_WITH -> getCriteria(part, property, value1, parameters, FilterOperation.ENDS_WITH);
            case CONTAINING -> getCriteria(part, property, value1, parameters, FilterOperation.CONTAINING);
            case NOT_CONTAINING -> getCriteria(part, property, value1, parameters, FilterOperation.NOT_CONTAINING);
            case WITHIN -> {
                value1 = Value.get(String.format("{ \"type\": \"AeroCircle\", \"coordinates\": [[%.8f, %.8f], %f] }",
                    value1, parameters.next(), parameters.next()));
                yield getCriteria(part, property, value1, parameters, FilterOperation.GEO_WITHIN);
            }
            case SIMPLE_PROPERTY -> getCriteria(part, property, value1, parameters, FilterOperation.EQ);
            case NEGATING_SIMPLE_PROPERTY -> getCriteria(part, property, value1, parameters, FilterOperation.NOTEQ);
            case IN -> getCriteria(part, property, value1, parameters, FilterOperation.IN);
            case NOT_IN -> getCriteria(part, property, value1, parameters, FilterOperation.NOT_IN);
            case TRUE -> getCriteria(part, property, true, parameters, FilterOperation.EQ);
            case FALSE -> getCriteria(part, property, false, parameters, FilterOperation.EQ);
            case EXISTS, IS_NOT_NULL -> getCriteria(part, property, value1, parameters, IS_NOT_NULL);
            case IS_NULL -> getCriteria(part, property, null, parameters, IS_NULL);
            default -> throw new IllegalArgumentException(String.format("Unsupported keyword '%s'", part.getType()));
        };
    }

    private Object convertIfNecessary(Object obj) {
        if (obj == null || obj instanceof AerospikeQueryCriteria || obj instanceof AerospikeNullQueryCriteria) {
            return obj;
        }

        // converting if necessary (e.g., Date to Long so that proper filter expression or sIndex filter can be built)
        final Object value = obj;
        TypeInformation<?> valueType = TypeInformation.of(value.getClass());
        return converter.toWritableValue(value, valueType);
    }

    public CriteriaDefinition getCriteria(Part part, AerospikePersistentProperty property, Object value1,
                                          Iterator<?> parametersIterator, FilterOperation op) {
        String fieldName = getFieldName(part.getProperty().getSegment(), property);
        Qualifier qualifier;

        Object value2 = null;
        if (part.getType() == BETWEEN || part.getType() == WITHIN) {
            value2 = parametersIterator.hasNext() ? convertIfNecessary(parametersIterator.next()) : null;
        }

        if (property.isIdProperty()) {
            qualifier = processId(value1);
        } else if (property.isCollectionLike()) {
            qualifier = processCollection(part, value1, value2, parametersIterator, op, fieldName);
        } else if (property.isMap()) {
            qualifier = processMap(part, value1, value2, parametersIterator, op, fieldName);
        } else {
            qualifier = processOther(part, value1, value2, parametersIterator, property, op, fieldName);
        }

        return qualifier;
    }

    private Qualifier processId(Object value1) {
        return getValidatedIdQualifier(value1);
    }

    private Qualifier getValidatedIdQualifier(Object value1) {
        if (value1 instanceof Collection<?>) {
            List<?> ids = ((Collection<?>) value1).stream().toList();
            return getIdInQualifier(ids);
        } else {
            return getIdEqualsQualifier(value1);
        }
    }

    private Qualifier getIdEqualsQualifier(Object value1) {
        Qualifier qualifier;
        if (value1 instanceof String) {
            qualifier = idEquals((String) value1);
        } else if (value1 instanceof Long) {
            qualifier = idEquals((Long) value1);
        } else if (value1 instanceof Integer) {
            qualifier = idEquals((Integer) value1);
        } else if (value1 instanceof Short) {
            qualifier = idEquals((Short) value1);
        } else if (value1 instanceof Byte) {
            qualifier = idEquals((Byte) value1);
        } else if (value1 instanceof Character) {
            qualifier = idEquals((Character) value1);
        } else if (value1 instanceof byte[]) {
            qualifier = idEquals((byte[]) value1);
        } else {
            throw new IllegalArgumentException("Invalid ID argument type: expected String, Number or byte[]");
        }
        return qualifier;
    }

    private Qualifier getIdInQualifier(List<?> ids) {
        Qualifier qualifier;
        Object firstId = ids.get(0);
        if (firstId instanceof String) {
            qualifier = idIn(ids.toArray(String[]::new));
        } else if (firstId instanceof Long) {
            qualifier = idIn(ids.toArray(Long[]::new));
        } else if (firstId instanceof Integer) {
            qualifier = idIn(ids.toArray(Integer[]::new));
        } else if (firstId instanceof Short) {
            qualifier = idIn(ids.toArray(Short[]::new));
        } else if (firstId instanceof Byte) {
            qualifier = idIn(ids.toArray(Byte[]::new));
        } else if (firstId instanceof Character) {
            qualifier = idIn(ids.toArray(Character[]::new));
        } else if (firstId instanceof byte[]) {
            qualifier = idIn(ids.toArray(byte[][]::new));
        } else {
            throw new IllegalArgumentException("Invalid ID argument type: expected String, Number or byte[]");
        }
        return qualifier;
    }

    private Qualifier processCollection(Part part, Object value1, Object value2, Iterator<?> parametersIterator,
                                        FilterOperation op, String fieldName) {
        List<Object> params = new ArrayList<>();
        parametersIterator.forEachRemaining(params::add);

        validateCollectionQuery(part.getProperty(), op, value1, value2, params);

        Qualifier.QualifierBuilder qb = Qualifier.builder();
        if (!params.isEmpty()) {
            if (op == FilterOperation.CONTAINING) {
                op = LIST_VAL_CONTAINING;
                params.add(0, value1); // value1 stores the first parameter
                return qualifierAndConcatenated(params, qb, part, fieldName, op, null);
            }
        } else if (!(value1 instanceof Collection<?>)) {
            op = getCorrespondingListFilterOperationOrFail(op);
        }

        return setQualifier(qb, fieldName, op, part, value1, value2, null, null);
    }

    private Object convertNullParameter(Object value) {
        return (value == NULL) ? null : value;
    }

    private void validateCollectionQuery(PropertyPath property, FilterOperation op, Object value1, Object value2,
                                         List<Object> params) {
        String queryPartDescription = String.join(" ", property.toString(), op.toString());
        switch (op) {
            case CONTAINING, NOT_CONTAINING -> validateCollectionQueryContaining(value1, params, queryPartDescription);
            case EQ, NOTEQ, GT, GTEQ, LT, LTEQ -> validateCollectionQueryComparison(value1, params,
                queryPartDescription);
            case BETWEEN -> validateCollectionQueryBetween(value1, value2, params, queryPartDescription);
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", op, property));
        }

        validateCollectionQueryTypes(property, queryPartDescription, value1, value2, params);
    }

    private void validateCollectionQueryTypes(PropertyPath property, String queryPartDescription, Object value1,
                                              Object value2, List<Object> params) {
        if (value1 instanceof Collection) {
            validateTypes(Collection.class, value1, value2, params, queryPartDescription);
        } else if (value1 instanceof AerospikeNullQueryCriteria) {
            // Not more than one null value
            if (getArgumentsSize(value1, value2, params) > 1) {
                throw new IllegalArgumentException(String.format("%s: invalid number of null arguments, expecting " +
                    "one", queryPartDescription));
            }
        }

        // Determining class of Collection's elements if possible
        Class<?> elementsClass = getCollectionElementsClass(property);
        if (elementsClass != null) {
            validateTypes(elementsClass, value1, value2, params, queryPartDescription);
        }
    }

    private Class<?> getCollectionElementsClass(PropertyPath property) {
        Type genericType = property.getTypeInformation().getType().getGenericSuperclass();
        Class<?> elementClass;
        if (genericType instanceof ParameterizedType) {
            // If it's a parameterized type, get the actual type arguments
            Type[] typeArguments = ((ParameterizedType) genericType).getActualTypeArguments();

            if (typeArguments.length > 0) {
                // The first type argument is the class of the elements in the Collection
                elementClass = (Class<?>) typeArguments[0];
            } else {
                // Collection has no generic type information
                elementClass = null;
            }
        } else {
            // Collection is not a parameterized type
            elementClass = null;
        }
        return elementClass;
    }

    private void validateCollectionQueryComparison(Object value1, List<Object> params, String queryPartDescription) {
        // Other than 1 argument
        if (getArgumentsSize(value1, params) != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }
    }

    private void validateCollectionQueryContaining(Object value1, List<Object> params, String queryPartDescription) {
        // No arguments
        if (getArgumentsSize(value1, params) == 0) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting at " +
                "least one");
        }
    }

    private void validateCollectionQueryBetween(Object value1, Object value2, List<Object> params,
                                                String queryPartDescription) {
        // No arguments
        if (getArgumentsSize(value1, value2, params) != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two");
        }
    }

    private Qualifier processMap(Part part, Object value1, Object value2, Iterator<?> parametersIterator,
                                 FilterOperation op,
                                 String fieldName) {
        List<Object> params = new ArrayList<>();
        parametersIterator.forEachRemaining(params::add);
        Qualifier qualifier = null;

        validateMapQuery(part.getProperty(), op, value1, value2, params);
//        validateType(part.getProperty(), op, value1, value2, params);

        // the first parameter is value1, params list contains parameters except value1 and value2
        if (params.size() == 1 || value2 != null) { // two parameters
            qualifier = processMap2Params(part, value1, value2, params, op, fieldName);
        } else if (params.isEmpty()) { // only value1 and/or value2 parameter(s)
            if (op != FilterOperation.BETWEEN) { // if not map in range (2 maps as parameters)
                // VALUE2 contains key (field name)
                qualifier = setQualifier(Qualifier.builder(), fieldName, op, part, value1, Value.get(fieldName),
                    null, null);
            }
        } else { // multiple parameters
            qualifier = processMapMultipleParams(part, value1, value2, params, op, fieldName);
        }

        return qualifier;
    }

    private void validateMapQuery(PropertyPath property, FilterOperation op, Object value1, Object value2,
                                  List<Object> params) {
        String queryPartDescription = String.join(" ", property.toString(), op.toString());
        switch (op) {
            case CONTAINING, NOT_CONTAINING -> validateMapQueryContaining(value1, params, queryPartDescription);
            case EQ, NOTEQ -> validateMapQueryEquals(value1, params, queryPartDescription);
            case GT, GTEQ, LT, LTEQ -> validateMapQueryComparison(value1, params, queryPartDescription);
            case BETWEEN -> validateMapQueryBetween(value1, value2, params, queryPartDescription);
            case LIKE, STARTS_WITH, ENDS_WITH -> validateMapQueryLike(value1, value2, params, queryPartDescription);
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", op, property));
        }
    }

    private void validateMapQueryContaining(Object value1, List<Object> params, String queryPartDescription) {
        // Less than two arguments, including a case when value1 intentionally equals null
        if (params.isEmpty()) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, at least two " +
                "arguments are required");
        }

        // Two or more arguments of type MapCriteria
        if (value1 instanceof AerospikeQueryCriteria && hasMultipleMapCriteria(value1, params)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, cannot " +
                "have multiple MapCriteria " +
                "arguments");
        }

        // Odd number of arguments when none are MapCriteria
        if (getArgumentsSize(value1, params) % 2 != 0 && !hasMapCriteria(value1, params)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, " +
                "one AerospikeMapCriteria argument is required");
        }

        // Even number of arguments, no MapCriteria, checking for allowed types in odd positions (keys)
        if (getArgumentsSize(value1, params) % 2 == 0 && !hasMapCriteria(value1, params)) {
            List<Object> list = new ArrayList<>(params);
            list.add(0, value1);
            for (int i = 0; i < list.size(); i += 2) {
                if (!(isAllowedMapKeyType(list.get(i)))) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid argument type, expected " +
                        "String, Number or byte[] at " +
                        "position " + (i + 1));
                }
            }
        }
    }

    private long getArgumentsSize(Object value1, Object value2, List<Object> params) {
        return Stream.of(value1, value2).filter(Objects::nonNull).count() + params.size();
    }

    private long getArgumentsSize(Object value1, List<Object> params) {
        int value1Size = value1 != null ? 1 : 0;
        return value1Size + params.size();
    }

    private long getArgumentsSize(Object value1, Object value2) {
        return Stream.of(value1, value2).filter(Objects::nonNull).count();
    }

    private boolean isAllowedMapKeyType(Object obj) {
        return obj instanceof String || obj instanceof Number || obj instanceof byte[] || obj == null;
    }

    private boolean hasMapCriteria(Object value1, List<Object> params) {
        return value1 instanceof AerospikeQueryCriteria || params.stream()
            .anyMatch(AerospikeQueryCriteria.class::isInstance);
    }

    private boolean hasMapCriteria(List<Object> params) {
        return params.stream().anyMatch(AerospikeQueryCriteria.class::isInstance);
    }

    private boolean hasMultipleMapCriteria(Object value1, List<Object> params) {
        return (value1 instanceof AerospikeQueryCriteria || hasMapCriteria(params))
            || (hasMultipleMapCriteria(params));
    }

    private boolean hasMultipleMapCriteria(List<Object> params) {
        return params.stream()
            .filter(AerospikeQueryCriteria.class::isInstance)
            .count() > 1;
    }

    private void validateMapQueryEquals(Object value1, List<Object> params, String queryPartDescription) {
        // Only one argument which is not a Map
        if (params.isEmpty() && !(value1 instanceof Map)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, expecting " +
                "either a Map or a " +
                "key-value pair");
        }

        // More than 2 arguments
        if (getArgumentsSize(value1, params) > 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting " +
                "either a Map or a key-value " +
                "pair");
        }

        // 2 arguments of type Map
        if (getArgumentsSize(value1, params) == 2 && getArgumentsMapsSize(value1, params) > 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, expecting " +
                "either a Map or a " +
                "key-value pair");
        }
    }

    private long getArgumentsMapsSize(Object value1, List<Object> params) {
        int value1MapCount = value1 instanceof Map ? 1 : 0;
        return value1MapCount + params.stream()
            .filter(Map.class::isInstance)
            .count();
    }

    private void validateMapQueryComparison(Object value1, List<Object> params, String queryPartDescription) {
        long argumentsSize = getArgumentsSize(value1, params);

        // More than two arguments
        if (argumentsSize > 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one " +
                "(Map) or two (Map key and " +
                "value)");
        }

        // One argument not of type Map
        if (argumentsSize == 1 && !(value1 instanceof Map)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, expecting " +
                "one (Map) or two (Map key" +
                " and value)");
        }

        // Two arguments, checking whether first argument's type is allowed
        if (argumentsSize == 2 && !isAllowedMapKeyType(value1)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid first argument type, expected " +
                "String, Number or byte[]");
        }
    }

    private void validateMapQueryBetween(Object value1, Object value2, List<Object> params,
                                         String queryPartDescription) {
        // Number of arguments is less than two or greater than three
        long argumentsSize = getArgumentsSize(value1, value2, params);
        if (argumentsSize < 2 || argumentsSize > 3) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two " +
                "(Maps) or three (Map key and two values)");
        }

        // Two arguments when at least one of them is not a Map
        if (queryHasOnlyBothValues(value1, value2, params)
            && (!(value1 instanceof Map) || !(value2 instanceof Map))) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, both must " +
                "be of type Map");
        }
    }

    private void validateMapQueryLike(Object value1, Object value2, List<Object> params, String queryPartDescription) {
        // Number of arguments is not two
        if (getArgumentsSize(value1, value2, params) != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, " +
                "expecting two (a key and an expression to compare with)");
        }

    }

    private boolean queryHasOnlyBothValues(Object value1, Object value2, List<Object> params) {
        return params.isEmpty() && value1 != null && value2 != null;
    }

    private Qualifier processMap2Params(Part part, Object value1, Object value2, List<Object> params,
                                        FilterOperation op, String fieldName) {
        Object nextParam = !params.isEmpty() ? convertIfNecessary(params.get(0)) : null; // nextParam is de facto the
        // second
        Qualifier qualifier;

        if (op == FilterOperation.CONTAINING) {
            qualifier = processMapContaining(nextParam, part, value1, fieldName, MAP_KEYS_CONTAIN, MAP_VALUES_CONTAIN,
                MAP_VAL_EQ_BY_KEY);
        } else if (op == FilterOperation.NOT_CONTAINING) {
            qualifier = processMapContaining(nextParam, part, value1, fieldName, MAP_KEYS_NOT_CONTAIN,
                MAP_VALUES_NOT_CONTAIN,
                MAP_VAL_NOTEQ_BY_KEY);
        } else {
            qualifier = processMapBetween(part, value1, value2, op, fieldName, nextParam);
        }

        return qualifier;
    }

    private Qualifier processMapContaining(Object nextParam, Part part, Object value1, String fieldName,
                                           FilterOperation keysOp, FilterOperation valuesOp, FilterOperation byKeyOp) {
        FilterOperation op;
        List<String> dotPath = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (nextParam instanceof AerospikeQueryCriteria onMap) {
            switch (onMap) {
                case KEY -> op = keysOp;
                case VALUE -> op = valuesOp;
                default -> throw new UnsupportedOperationException("Unsupported parameter: " + onMap);
            }
        } else {
            op = byKeyOp;
            dotPath = List.of(part.getProperty().toDotPath(), Value.get(value1).toString());
            setQbValuesForMapByKey(qb, value1, nextParam);
        }

        return setQualifier(qb, fieldName, op, part, value1, null, null, dotPath);
    }

    private Qualifier processMapBetween(Part part, Object value1, Object value2, FilterOperation op, String fieldName
        , Object nextParam) {
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (nextParam != null) {
            if (op == FilterOperation.BETWEEN) { // BETWEEN for values by a certain key or for 2 Maps
                op = getCorrespondingMapValueFilterOperationOrFail(op);
                qb.setValue2(Value.get(value1)); // contains key
                qb.setValue1(Value.get(value2)); // contains lower limit (inclusive)
                qb.setValue3(Value.get(nextParam)); // contains upper limit (inclusive)
            } else {
                if (op == FilterOperation.EQ) {
                    throw new IllegalArgumentException(String.format("Unsupported arguments '%s' and '%s', expecting " +
                        "Map argument in findByMapEquals queries", value1, nextParam));
                } else {
                    op = getCorrespondingMapValueFilterOperationOrFail(op);
                    setQbValuesForMapByKey(qb, value1, nextParam);
                }
            }
        }

        List<String> dotPath = List.of(part.getProperty().toDotPath(), Value.get(value1).toString());
        return setQualifier(qb, fieldName, op, part, value1, value2, null, dotPath);
    }

    private Qualifier processMapMultipleParams(Part part, Object value1, Object value2, List<Object> params,
                                               FilterOperation op, String fieldName) {
        if (op == FilterOperation.CONTAINING) {
            return processMapMultipleParamsContaining(part, value1, value2, params, op, fieldName);
        } else {
            String paramsString = params.stream().map(Object::toString).collect(Collectors.joining(", "));
            throw new IllegalArgumentException(String.format(
                "Expected not more than 2 arguments (propertyType: Map, filterOperation: %s), got %d instead: '%s, %s'",
                op, params.size() + 1, value1, paramsString));
        }
    }

    private Qualifier processMapMultipleParamsContaining(Part part, Object value1, Object value2, List<Object> params,
                                                         FilterOperation op, String fieldName) {
        List<String> dotPath = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (params.get(params.size() - 1) instanceof AerospikeQueryCriteria mapCriteria) {
            switch (mapCriteria) {
                case KEY -> op = MAP_KEYS_CONTAIN;
                case VALUE -> op = MAP_VALUES_CONTAIN;
                case VALUE_CONTAINING -> op = MAP_VAL_CONTAINING_BY_KEY;
            }
            params = params.stream().limit(params.size() - 1L).collect(Collectors.toList());
        } else {
            op = MAP_VAL_EQ_BY_KEY;
            dotPath = List.of(part.getProperty().toDotPath(), Value.get(value1).toString());
        }

        params.add(0, value1); // value1 stores the first parameter
        if (op == MAP_VAL_CONTAINING_BY_KEY || op == MAP_VAL_EQ_BY_KEY) {
            return processMapMultipleParamsContainingPerSize(params, qb, part, value1, value2, fieldName, op, dotPath);
        } else {
            return qualifierAndConcatenated(params, qb, part, fieldName, op, dotPath);
        }
    }

    private Qualifier processMapMultipleParamsContainingPerSize(List<Object> params, Qualifier.QualifierBuilder qb,
                                                                Part part, Object value1, Object value2,
                                                                String fieldName, FilterOperation op,
                                                                List<String> dotPath) {
        if (params.size() > 2) {
            if ((params.size() & 1) != 0) { // if params.size() is an odd number
                throw new IllegalArgumentException("FindByMapContaining: expected either one, two " +
                    "or even number of key/value arguments, instead got " + params.size());
            }
            return qualifierAndConcatenated(params, qb, part, fieldName, op, dotPath, true);
        } else if (params.size() == 2) {
            setQbValuesForMapByKey(qb, params.get(0), params.get(1));
            return setQualifier(qb, fieldName, op, part, value1, value2, null, null);
        } else {
            throw new UnsupportedOperationException("Unsupported combination of operation " + op + " and " +
                "parameters with size of + " + params.size());
        }
    }

    private Qualifier processOther(Part part, Object value1, Object value2, Iterator<?> parametersIterator,
                                   AerospikePersistentProperty property, FilterOperation op, String fieldName) {
        List<String> dotPath = null;
        if (value2 == null && parametersIterator.hasNext()) {
            value2 = convertIfNecessary(parametersIterator.next());
        }

        Object value3 = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (part.getProperty().hasNext()) { // if it is a POJO field (a simple type field or an inner POJO)
            PropertyPath nestedProperty = getNestedPropertyPath(part.getProperty());
            if (isPojo(nestedProperty.getType())) {
                validatePojoQuery(nestedProperty, op, value1, value2);
            } else {
                validateSimplePropertyQuery(nestedProperty, op, value1, value2);
            }

            if (op == FilterOperation.BETWEEN) {
                value3 = Value.get(value2); // contains upper limit
            } else if (op == IS_NOT_NULL || op == IS_NULL) {
                value1 = Value.get(property.getFieldName()); // contains key (field name)
            }

            // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
            op = getCorrespondingMapValueFilterOperationOrFail(op);
            value2 = Value.get(property.getFieldName()); // VALUE2 contains key (field name)
            dotPath = List.of(part.getProperty().toDotPath());
        } else if (isPojo(part.getProperty().getType())) { // if it is a first level POJO
            validatePojoQuery(part.getProperty(), op, value1, value2);

            if (op != FilterOperation.BETWEEN) {
                // if it is a POJO compared for equality it already has op == FilterOperation.EQ
                value2 = Value.get(property.getFieldName()); // VALUE2 contains key (field name)
            }
        } else {
            validateSimplePropertyQuery(part.getProperty(), op, value1, value2);
        }

        return setQualifier(qb, fieldName, op, part, value1, value2, value3, dotPath);
    }

    private void validateTypes(PropertyPath property, FilterOperation op, Object value1, Object value2) {
        String queryPartDescription = String.join(" ", property.toString(), op.toString());
        Class<?> propertyType = property.getTypeInformation().getType();
        if (!(isAssignableValueOrConverted(propertyType, value1)) || !(isAssignableValueOrConverted(propertyType,
            value2))) {
            throw new IllegalArgumentException(String.format("%s: Type mismatch, expecting %s", queryPartDescription,
                propertyType.getSimpleName()));
        }
    }

    private void validateTypes(PropertyPath property, String queryPartDescription, Object value1, Object value2) {
        Class<?> propertyType = property.getTypeInformation().getType();
        if (!(isAssignableValueOrConverted(propertyType, value1)) || !(isAssignableValueOrConverted(propertyType,
            value2))) {
            throw new IllegalArgumentException(String.format("%s: Type mismatch, expecting %s", queryPartDescription,
                propertyType.getSimpleName()));
        }
    }

    private boolean isAssignableValueOrConverted(Class<?> propertyType, Object obj) {
        return obj == null || isAssignableValue(propertyType, obj)
            || converter.getCustomConversions().hasCustomReadTarget(obj.getClass(), propertyType);
    }

    private void validateTypes(PropertyPath property, FilterOperation op, Object value1, Object value2,
                               List<Object> params) {
        String queryPartDescription = String.join(" ", property.toString(), op.toString());
        Class<?> propertyType = property.getTypeInformation().getType();
        params.add(0, value1);
        params.add(1, value2);
        if (!params.stream().allMatch(param -> isAssignableValueOrConverted(propertyType, param))) {
            throw new IllegalArgumentException(String.format("%s: Type mismatch, expecting %s", queryPartDescription,
                propertyType.getSimpleName()));
        }
    }

    private void validateTypes(Class<?> propertyType, Object value1, Object value2, String queryPartDescription) {
        List<Object> params = new ArrayList<>();
        validateTypes(propertyType, value1, value2, params, queryPartDescription);
    }

    private void validateTypes(Class<?> propertyType, Object value1, Object value2, List<Object> params,
                               String queryPartDescription) {
        params.add(0, value1);
        params.add(1, value2);
        if (!params.stream().allMatch(param -> isAssignableValueOrConverted(propertyType, param))) {
            throw new IllegalArgumentException(String.format("%s: Type mismatch, expecting %s", queryPartDescription,
                propertyType.getSimpleName()));
        }
    }

    /**
     * Iterate over nested properties until the current one
     */
    private PropertyPath getNestedPropertyPath(PropertyPath propertyPath) {
        PropertyPath result = null;
        for (PropertyPath current = propertyPath; current != null; current = current.next()) {
            result = current;
        }
        return result;
    }

    private void validatePojoQuery(PropertyPath property, FilterOperation op, Object value1, Object value2) {
        String queryPartDescription = String.join(" ", property.toString(), op.toString());
        switch (op) {
            case CONTAINING, NOT_CONTAINING -> throw new UnsupportedOperationException("Unsupported operation, " +
                "please use queries like 'findByPojoField()' directly addressing the required fields");
            case EQ, NOTEQ -> validatePojoQueryEquals(value1, queryPartDescription);
            case GT, GTEQ, LT, LTEQ -> validatePojoQueryComparison(value1, queryPartDescription);
            case BETWEEN -> validatePojoQueryBetween(value1, value2, queryPartDescription);
            case IN, NOT_IN -> validatePojoQueryIn(value1, value2, queryPartDescription);
            case IS_NOT_NULL, IS_NULL -> validatePojoQueryIsNull(value1, value2, queryPartDescription);
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", op, property));
        }

        validateTypes(property, op, value1, value2);
    }

    private void validateSimplePropertyQuery(PropertyPath property, FilterOperation op, Object value1, Object value2) {
        String queryPartDescription = String.join(" ", property.toString(), op.toString());
        switch (op) {
            case CONTAINING, NOT_CONTAINING, GT, GTEQ, LT, LTEQ, LIKE, STARTS_WITH, ENDS_WITH -> {
                validateSimplePropertyQueryComparison(queryPartDescription, value1, value2);
                validateTypes(property, queryPartDescription, value1, value2);
            }
            case IN, NOT_IN -> {
                validateSimplePropertyQueryComparison(queryPartDescription, value1, value2);
                validateSimplePropertyInQueryTypes(property, queryPartDescription, value1, value2);
            }
            case EQ, NOTEQ -> {
                validateSimplePropertyQueryEquals(property.getType(), queryPartDescription, value1, value2);
                validateTypes(property, queryPartDescription, value1, value2);
            }
            case BETWEEN -> {
                validateSimplePropertyQueryBetween(queryPartDescription, value1, value2);
                validateTypes(property, queryPartDescription, value1, value2);
            }
            case IS_NOT_NULL, IS_NULL -> {
                validateSimplePropertyQueryIsNull(queryPartDescription, value1, value2);
                validateTypes(property, queryPartDescription, value1, value2);
            }
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", op, property));
        }
    }

    private void validateSimplePropertyInQueryTypes(PropertyPath property, String queryPartDescription, Object value1
        , Object value2) {
        if (value1 instanceof Collection) {
            validateTypes(Collection.class, value1, value2, queryPartDescription);
        } else if (value1 instanceof AerospikeNullQueryCriteria) {
            // Not more than one null value
            if (getArgumentsSize(value1, value2) > 1) {
                throw new IllegalArgumentException(queryPartDescription + ": invalid number of null arguments, " +
                    "expecting one");
            }
        } else {
            validateTypes(property, queryPartDescription, value1, value2);
        }
    }

    private void validateSimplePropertyQueryEquals(Class<?> type, String queryPartDescription, Object value1,
                                                   Object value2) {
        if (isBoolean(type)) {
            // Other than one boolean argument
            if (getArgumentsSize(value1, value2) != 1) {
                throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, " +
                    "expecting one");
            }
        } else {
            // No arguments
            if (getArgumentsSize(value1, value2) == 0) {
                throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting " +
                    "at least one");
            }
        }
    }

    private void validateSimplePropertyQueryComparison(String queryPartDescription, Object value1, Object value2) {
        // Number of arguments is not one
        if (getArgumentsSize(value1, value2) != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }
    }

    private void validateSimplePropertyQueryBetween(String queryPartDescription, Object value1, Object value2) {
        // Number of arguments is not two
        if (getArgumentsSize(value1, value2) != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two " +
                "POJOs");
        }
    }

    private void validateSimplePropertyQueryIsNull(String queryPartDescription, Object value1, Object value2) {
        // Number of arguments is not zero
        if (getArgumentsSize(value1, value2) != 0) {
            throw new IllegalArgumentException(queryPartDescription + ": expecting no arguments");
        }
    }

    private void validatePojoQueryEquals(Object value1, String queryPartDescription) {
        // No arguments
        if (value1 == null) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one " +
                "POJO");
        }

        // Checking whether the argument is of the following type:
        // a primitive or primitive wrapper, an Enum, a String or other CharSequence, a Number, a Date, a Temporal,
        // a UUID, a URI, a URL, a Locale, or a Class
        Class<?> class1 = value1.getClass();
        if (isSimpleValueType(class1)) {
            throw new IllegalArgumentException(String.format("%s: invalid arguments type, expecting a POJO, instead " +
                "got %s", queryPartDescription, class1.getSimpleName()));
        }
    }

    private void validatePojoQueryComparison(Object value1, String queryPartDescription) {
        // No arguments
        if (value1 == null) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one " +
                "POJO");
        }

        // Checking whether the argument is of the following type:
        // a primitive or primitive wrapper, an Enum, a String or other CharSequence, a Number, a Date, a Temporal,
        // a UUID, a URI, a URL, a Locale, or a Class
        Class<?> class1 = value1.getClass();
        if (isSimpleValueType(class1)) {
            throw new IllegalArgumentException(String.format("%s: invalid arguments type: expecting a POJO, instead " +
                "got %s", queryPartDescription, class1.getSimpleName()));
        }
    }

    private void validatePojoQueryBetween(Object value1, Object value2, String queryPartDescription) {
        // Number of arguments is not two
        if (getArgumentsSize(value1, value2) != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two " +
                "POJOs");
        }

        // Checking whether at least one of the arguments is of the following type:
        // a primitive or primitive wrapper, an Enum, a String or other CharSequence, a Number, a Date, a Temporal,
        // a UUID, a URI, a URL, a Locale, or a Class
        Class<?> class1 = value1.getClass();
        Class<?> class2 = value2.getClass();
        if (isSimpleValueType(class1) || isSimpleValueType(class2)) {
            throw new IllegalArgumentException(String.format("%s: invalid arguments type, expecting two POJOs, " +
                "instead got %s and %s", queryPartDescription, class1.getSimpleName(), class2.getSimpleName()));
        }
    }

    private void validatePojoQueryIn(Object value1, Object value2, String queryPartDescription) {
        // Number of arguments is not one
        if (getArgumentsSize(value1, value2) != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }
    }

    private void validatePojoQueryIsNull(Object value1, Object value2, String queryPartDescription) {
        // Number of arguments is not zero
        if (getArgumentsSize(value1, value2) != 0) {
            throw new IllegalArgumentException(queryPartDescription + ": expecting no arguments");
        }
    }

    private String getFieldName(String segmentName, AerospikePersistentProperty property) {
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

    private Qualifier qualifierAndConcatenated(List<Object> params, Qualifier.QualifierBuilder qb,
                                               Part part, String fieldName, FilterOperation op,
                                               List<String> dotPath) {
        return qualifierAndConcatenated(params, qb, part, fieldName, op, dotPath, false);
    }

    private Qualifier qualifierAndConcatenated(List<Object> params, Qualifier.QualifierBuilder qb,
                                               Part part, String fieldName, FilterOperation op,
                                               List<String> dotPath, boolean containingMapKeyValuePairs) {
        Qualifier[] qualifiers;
        if (containingMapKeyValuePairs) {
            qualifiers = new Qualifier[params.size() / 2]; // keys/values qty must be even
            for (int i = 0, j = 0; i < params.size(); i += 2, j++) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i + 1));
                qualifiers[j] = setQualifier(qb, fieldName, op, part, params.get(i),
                    null, null, dotPath);
            }

            return Qualifier.and(qualifiers);
        } else {
            qualifiers = new Qualifier[params.size()];
            for (int i = 0; i < params.size(); i++) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i));
                qualifiers[i] = setQualifier(qb, fieldName, op, part, params.get(i),
                    null, null, dotPath);
            }
        }

        return Qualifier.and(qualifiers);
    }

    private Qualifier setQualifier(Qualifier.QualifierBuilder qb, String fieldName, FilterOperation op, Part part,
                                   Object value1, Object value2, Object value3, List<String> dotPath) {
        value1 = convertNullParameter(value1);
        value2 = convertNullParameter(value2);
        value3 = convertNullParameter(value3);

        qb.setField(fieldName)
            .setFilterOperation(op)
            .setIgnoreCase(ignoreCaseToBoolean(part))
            .setConverter(converter);
        setNotNullQbValues(qb, value1, value2, value3, dotPath);
        return qb.build();
    }

    private FilterOperation getCorrespondingMapValueFilterOperationOrFail(FilterOperation op) {
        try {
            return FilterOperation.valueOf("MAP_VAL_" + op + "_BY_KEY");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot find corresponding MAP_VAL_..._BY_KEY FilterOperation for '" + op + "'");
        }
    }

    private FilterOperation getCorrespondingListFilterOperationOrFail(FilterOperation op) {
        try {
            return FilterOperation.valueOf("LIST_VAL_" + op);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot find corresponding LIST_VAL_... FilterOperation for '" + op + "'");
        }
    }

    private void setNotNullQbValues(Qualifier.QualifierBuilder qb, Object v1, Object v2, Object v3,
                                    List<String> dotPath) {
        if (v1 != null && !qb.hasValue1()) qb.setValue1(Value.get(v1));
        if (v2 != null && !qb.hasValue2()) qb.setValue2(Value.get(v2));
        if (v3 != null && !qb.hasValue3()) qb.setValue3(Value.get(v3));
        if (dotPath != null && !qb.hasDotPath()) qb.setDotPath(dotPath);
    }

    private void setQbValuesForMapByKey(Qualifier.QualifierBuilder qb, Object key, Object value) {
        qb.setValue1(Value.get(value)); // contains value
        qb.setValue2(Value.get(key)); // contains key
    }

    private boolean isPojo(Class<?> clazz) { // if it is a first level POJO or a Map
        TypeInformation<?> type = TypeInformation.of(clazz);
        return !Utils.isSimpleValueType(clazz) && !type.isCollectionLike();
    }

    @Override
    protected CriteriaDefinition and(Part part, CriteriaDefinition base, Iterator<Object> iterator) {
        CriteriaDefinition criteriaDefinition = create(part, iterator);

        if (base == null) {
            return criteriaDefinition;
        }

        return Qualifier.and(base.getCriteriaObject(), criteriaDefinition.getCriteriaObject());
    }

    @Override
    protected CriteriaDefinition or(CriteriaDefinition base, CriteriaDefinition criteria) {
        return Qualifier.or(base.getCriteriaObject(), criteria.getCriteriaObject());
    }

    @Override
    protected Query complete(CriteriaDefinition criteria, Sort sort) {
        Query query = criteria == null ? null : new Query(criteria).with(sort);

        if (LOG.isDebugEnabled() && criteria != null) {
            logQualifierDetails(criteria);
        }

        return query;
    }

    private void logQualifierDetails(CriteriaDefinition criteria) {
        Qualifier qualifier = criteria.getCriteriaObject();
        Qualifier[] qualifiers = qualifier.getQualifiers();
        if (qualifiers != null && qualifiers.length > 0) {
            Arrays.stream(qualifiers).forEach(this::logQualifierDetails);
        }

        String field = (StringUtils.hasLength(qualifier.getField()) ? qualifier.getField() : "");
        String operation = (StringUtils.hasLength(qualifier.getOperation().toString()) ?
            qualifier.getOperation().toString() : "N/A");
        String value1 = (qualifier.getValue1() != null && !qualifier.getValue1().toString().isEmpty() ?
            qualifier.getValue1().toString() : "");
        String value2 = (qualifier.getValue2() != null && !qualifier.getValue2().toString().isEmpty() ?
            qualifier.getValue2().toString() : "");

        LOG.debug("Created query: {} {} {} {}", field, operation, value1, value2);
    }

    private boolean ignoreCaseToBoolean(Part part) {
        return switch (part.shouldIgnoreCase()) {
            case WHEN_POSSIBLE -> part.getProperty().getType() == String.class;
            case ALWAYS -> true;
            default -> false;
        };
    }
}
