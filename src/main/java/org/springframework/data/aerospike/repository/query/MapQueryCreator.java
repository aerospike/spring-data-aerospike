package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.springframework.data.aerospike.query.FilterOperation.MAP_KEYS_CONTAIN;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_KEYS_NOT_CONTAIN;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VALUES_CONTAIN;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VALUES_NOT_CONTAIN;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VAL_CONTAINING_BY_KEY;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VAL_EQ_BY_KEY;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VAL_NOTEQ_BY_KEY;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.convertIfNecessary;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getCorrespondingMapValueFilterOperationOrFail;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getElementsClass;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.isAssignableValueOrConverted;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.qualifierAndConcatenated;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQbValuesForMapByKey;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateTypes;
import static org.springframework.util.ClassUtils.isAssignable;
import static org.springframework.util.ClassUtils.isAssignableValue;

public class MapQueryCreator implements IAerospikeQueryCreator {

    private final Part part;
    private final AerospikePersistentProperty property;
    private final String fieldName;
    private final List<Object> queryParameters;
    private final FilterOperation filterOperation;
    private final MappingAerospikeConverter converter;

    public MapQueryCreator(Part part, AerospikePersistentProperty property, String fieldName,
                           List<Object> queryParameters, FilterOperation filterOperation,
                           MappingAerospikeConverter converter) {
        this.part = part;
        this.property = property;
        this.fieldName = fieldName;
        this.queryParameters = queryParameters;
        this.filterOperation = filterOperation;
        this.converter = converter;
    }

    @Override
    public void validate() {
        String queryPartDescription = String.join(" ", part.getProperty().toString(), filterOperation.toString());
        switch (filterOperation) {
            case CONTAINING, NOT_CONTAINING -> validateMapQueryContaining(queryParameters, queryPartDescription);
            case EQ, NOTEQ -> validateMapQueryEquals(queryParameters, queryPartDescription);
            case GT, GTEQ, LT, LTEQ -> validateMapQueryComparison(queryParameters, queryPartDescription);
            case BETWEEN -> validateMapQueryBetween(queryParameters, queryPartDescription);
            case LIKE, STARTS_WITH, ENDS_WITH -> validateMapQueryLike(queryParameters, queryPartDescription);
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", filterOperation, property));
        }

//        validateMapQueryTypes(part.getProperty(), queryPartDescription, queryParameters); // TODO
    }

    private void validateMapQueryContaining(List<Object> queryParameters, String queryPartDescription) {
        // Less than two arguments, including a case when value1 intentionally equals null
        if (queryParameters.isEmpty()) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, at least two " +
                "arguments are required");
        }

        Object value1 = queryParameters.get(0);
        // Two or more arguments of type MapCriteria
        if (value1 instanceof CriteriaDefinition.AerospikeQueryCriteria && hasMultipleMapCriteria(queryParameters)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, cannot " +
                "have multiple MapCriteria " +
                "arguments");
        }

        // Odd number of arguments when none are MapCriteria
        if (queryParameters.size() % 2 != 0 && !hasMapCriteria(queryParameters)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, " +
                "one AerospikeMapCriteria argument is required");
        }

        // Even number of arguments, no MapCriteria, checking for allowed types in odd positions (keys)
        if (queryParameters.size() % 2 == 0 && !hasMapCriteria(queryParameters)) {
            List<Object> list = new ArrayList<>(queryParameters);
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

    private boolean hasMultipleMapCriteria(List<Object> params) {
        return params.stream()
            .filter(CriteriaDefinition.AerospikeQueryCriteria.class::isInstance)
            .count() > 1;
    }

    private boolean hasMapCriteria(List<Object> params) {
        return params.stream().anyMatch(CriteriaDefinition.AerospikeQueryCriteria.class::isInstance);
    }

    private boolean isAllowedMapKeyType(Object obj) {
        return obj instanceof String || obj instanceof Number || obj instanceof byte[] || obj == null;
    }

    private void validateMapQueryEquals(List<Object> queryParameters, String queryPartDescription) {
        Object value1 = queryParameters.get(0);
        // Only one argument which is not a Map
        if (queryParameters.isEmpty() && !(value1 instanceof Map)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, expecting " +
                "either a Map or a " +
                "key-value pair");
        }

        // More than 2 arguments
        if (queryParameters.size() > 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting " +
                "either a Map or a key-value " +
                "pair");
        }

        // 2 arguments of type Map
        if (queryParameters.size() == 2 && getArgumentsMapsSize(queryParameters) > 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, expecting " +
                "either a Map or a " +
                "key-value pair");
        }
    }

    private long getArgumentsMapsSize(List<Object> queryParameters) {
        return queryParameters.stream()
            .filter(Map.class::isInstance)
            .count();
    }

    private void validateMapQueryComparison(List<Object> params, String queryPartDescription) {
        int argumentsSize = queryParameters.size();

        // More than two arguments
        if (argumentsSize > 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one " +
                "(Map) or two (Map key and " +
                "value)");
        }

        Object value1 = params.get(0);
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

    private void validateMapQueryBetween(List<Object> params, String queryPartDescription) {
        // Number of arguments is less than two or greater than three
        int argumentsSize = queryParameters.size();
        if (argumentsSize < 2 || argumentsSize > 3) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two " +
                "(Maps) or three (Map key and two values)");
        }

        Object value1 = queryParameters.get(0);
        Object value2 = queryParameters.get(1);
        // Two arguments when at least one of them is not a Map
        if (queryHasOnlyTwoValues(params) && (!(value1 instanceof Map) || !(value2 instanceof Map))) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid combination of arguments, both must " +
                "be of type Map");
        }
    }

    private boolean queryHasOnlyTwoValues(List<Object> params) {
        return params.size() == 2;
    }

    private void validateMapQueryTypes(PropertyPath property, String queryPartDescription, List<Object> params) {
        Object value1 = params.get(0);
        Object value2 = params.get(1);

        if (value1 instanceof Map) {
            validateTypes(converter, Map.class, params, queryPartDescription);
        } else {
            // Determining class of Map's elements
            Class<?> elementsClass = getElementsClass(property); // TODO: both keys and values? add NULL check
            if (elementsClass != null) {
                validateMapTypes(elementsClass, params, queryPartDescription, "Map");
            }
        }
    }

    private void validateMapTypes(Class<?> propertyType, List<Object> params,
                                  String queryPartDescription, String... alternativeTypes) {
        List<Object> parameters = params.stream().filter(Objects::nonNull).toList();
        if (params == null || params.size() == 0) return;

        Object value1 = params.get(0);
        // Checking versus Number rather than strict type to be able to compare, e.g., integers to a long
        if (isAssignable(Number.class, propertyType) && isAssignableValue(Number.class, value1))
            propertyType = Number.class;

        Class<?> clazz = propertyType;
        if (!parameters.stream().allMatch(param -> isAssignableValueOrConverted(clazz, param, converter))) {
            String validTypes = propertyType.getSimpleName();
            if (alternativeTypes.length > 0) {
                validTypes = String.format("one of the following types: %s", propertyType.getSimpleName() + ", "
                    + String.join(", ", alternativeTypes));
            }
            throw new IllegalArgumentException(String.format("%s: Type mismatch, expecting %s", queryPartDescription,
                validTypes));
        }
    }

    private void validateMapQueryLike(List<Object> params, String queryPartDescription) {
        // Number of arguments is not two
        if (params.size() != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, " +
                "expecting two (a key and an expression to compare with)");
        }

    }

    @Override
    public Qualifier process() {
        Qualifier qualifier = null;
        int paramsSize = queryParameters.size();

        // the first parameter is value1, params list contains parameters except value1 and value2
        if (paramsSize == 2 || paramsSize == 3) { // two or three parameters
            qualifier = processMap3Params(part, queryParameters, filterOperation, fieldName);
        } else if (queryParameters.size() < 2) { // only value1 and/or value2 parameter(s)
            if (filterOperation != FilterOperation.BETWEEN) { // if not map in range (2 maps as parameters)
                // VALUE2 contains key (field name)
                Object value1 = queryParameters.get(0);
                qualifier = setQualifier(converter, Qualifier.builder(), fieldName, filterOperation, part, null,
                    queryParameters);
            }
        } else { // multiple parameters
            qualifier = processMapMultipleParams(part, queryParameters, filterOperation, fieldName);
        }

        return qualifier;
    }

    private Qualifier processMap3Params(Part part, List<Object> params, FilterOperation op, String fieldName) {
        Qualifier qualifier;
        Object value1 = queryParameters.get(0);
        Object value2 = queryParameters.get(1);
        Object nextParam = params.size() > 2 ? convertIfNecessary(params.get(2), converter) : null;

        if (op == FilterOperation.CONTAINING) {
            qualifier = processMapContaining(nextParam, part, value1, fieldName, MAP_KEYS_CONTAIN, MAP_VALUES_CONTAIN,
                MAP_VAL_EQ_BY_KEY);
        } else if (op == FilterOperation.NOT_CONTAINING) {
            qualifier = processMapContaining(nextParam, part, value1, fieldName, MAP_KEYS_NOT_CONTAIN,
                MAP_VALUES_NOT_CONTAIN, MAP_VAL_NOTEQ_BY_KEY);
        } else {
            qualifier = processMapOtherThanContaining(part, params, op, fieldName);
        }

        return qualifier;
    }

    private Qualifier processMapContaining(Object nextParam, Part part, Object value1, String fieldName,
                                           FilterOperation keysOp, FilterOperation valuesOp, FilterOperation byKeyOp) {
        FilterOperation op;
        List<String> dotPath = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (nextParam instanceof CriteriaDefinition.AerospikeQueryCriteria onMap) {
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

        return setQualifier(converter, qb, fieldName, op, part, dotPath, queryParameters);
    }

    private Qualifier processMapOtherThanContaining(Part part, List<Object> queryParameters, FilterOperation op,
                                                    String fieldName) {
        Qualifier.QualifierBuilder qb = Qualifier.builder();
        Object value1 = queryParameters.get(0);

//        if (queryParameters.size() == 3) {
            if (op == FilterOperation.BETWEEN) { // BETWEEN for values by a certain key or for 2 Maps
                op = getCorrespondingMapValueFilterOperationOrFail(op);
//                qb.setValue2(Value.get(value1)); // contains key
//                qb.setValue1(Value.get(value2)); // contains lower limit (inclusive)
//                qb.setValue3(Value.get(nextParam)); // contains upper limit (inclusive)
            } else {
                op = getCorrespondingMapValueFilterOperationOrFail(op);
            }
//        }

        List<String> dotPath = List.of(part.getProperty().toDotPath(), Value.get(value1).toString());
        return setQualifier(converter, qb, fieldName, op, part, dotPath, queryParameters);
    }

    private Qualifier processMapMultipleParams(Part part, List<Object> params, FilterOperation op, String fieldName) {
        Object value1 = params.get(0);
        Object value2 = params.get(1);

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

        if (params.get(params.size() - 1) instanceof CriteriaDefinition.AerospikeQueryCriteria mapCriteria) {
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
            return qualifierAndConcatenated(converter, params, qb, part, fieldName, op, dotPath, queryParameters);
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
            return qualifierAndConcatenated(converter, params, qb, part, fieldName, op, dotPath, true, queryParameters);
        } else if (params.size() == 2) {
            setQbValuesForMapByKey(qb, params.get(0), params.get(1));
            return setQualifier(converter, qb, fieldName, op, part, null, queryParameters);
        } else {
            throw new UnsupportedOperationException("Unsupported combination of operation " + op + " and " +
                "parameters with size of + " + params.size());
        }
    }

}
