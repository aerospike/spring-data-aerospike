package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.query.qualifier.QualifierBuilder;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.util.TypeInformation;

import java.util.List;
import java.util.Map;

import static org.springframework.data.aerospike.query.FilterOperation.*;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getCorrespondingMapValueFilterOperationOrFail;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getValueOfQueryParameter;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.isAssignableValueOrConverted;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.qualifierAndConcatenated;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderKey;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderSecondValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderValue;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion.NULL_PARAM;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY_VALUE_PAIR;

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
            case CONTAINING, NOT_CONTAINING -> validateMapQueryContaining(queryPartDescription);
            case EQ, NOTEQ, GT, GTEQ, LT, LTEQ -> validateMapQueryComparison(queryPartDescription);
            case BETWEEN -> validateMapQueryBetween(queryPartDescription);
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", filterOperation, property));
        }
    }

    private void validateMapQueryContaining(String queryPartDescription) {
        // Less than two arguments
        if (queryParameters.size() < 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, at least two " +
                "arguments are required");
        }

        Object param1 = queryParameters.get(0);
        // The first argument not QueryCriterion
        if (!(param1 instanceof AerospikeQueryCriterion)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid first argument type, required " +
                "AerospikeQueryCriterion");
        }

        Object param2 = queryParameters.get(1);
        switch ((AerospikeQueryCriterion) param1) {
            case KEY -> {
                if (queryParameters.size() != 2) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, " +
                        "expecting two");
                }

                if (!(isValidMapKeyTypeOrUnresolved(part.getProperty().getTypeInformation(), param2))) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid map key type at position 2");
                }
            }
            case VALUE -> {
                if (queryParameters.size() != 2) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, " +
                        "expecting two");
                }

                if (!(isValidMapValueTypeOrUnresolved(part.getProperty().getTypeInformation(), param2))) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid map value type at position 2");
                }
            }
            case KEY_VALUE_PAIR -> {
                if (queryParameters.size() != 3) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, " +
                        "expecting three");
                }

                if (!(isValidMapKeyTypeOrUnresolved(part.getProperty().getTypeInformation(), param2))) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid map key type at position 2");
                }
                Object param3 = queryParameters.get(2);
                if (!(isValidMapValueTypeOrUnresolved(part.getProperty().getTypeInformation(), param3))) {
                    throw new IllegalArgumentException(queryPartDescription + ": invalid map value type at position 3");
                }
            }
            default -> throw new IllegalStateException(queryPartDescription + ": invalid AerospikeQueryCriterion " +
                "type of the first argument, expecting KEY, VALUE or KEY_VALUE_PAIR");
        }
    }

    private boolean isValidMapKeyTypeOrUnresolved(TypeInformation<?> typeInformation, Object param) {
        Class<?> mapKeyClass;
        try {
            if (typeInformation.getComponentType() == null) {
                return true;
            }
            mapKeyClass = typeInformation.getComponentType().getType();
        } catch (IllegalStateException e) { // cannot resolve Map key type
            mapKeyClass = null;
        }
        return mapKeyClass == null || isAssignableValueOrConverted(mapKeyClass, param, converter);
    }

    private boolean isValidMapValueTypeOrUnresolved(TypeInformation<?> typeInformation, Object param) {
        Class<?> mapValueClass;
        try {
            if (typeInformation.getRequiredMapValueType() == null) {
                return true;
            }
            mapValueClass = typeInformation.getRequiredMapValueType().getType();
        } catch (IllegalStateException e) { // cannot resolve Map value type
            mapValueClass = null;
        }
        return mapValueClass == null || isAssignableValueOrConverted(mapValueClass, param, converter)
            || param == NULL_PARAM;
    }

    private void validateMapQueryComparison(String queryPartDescription) {
        // Other than 1 argument
        if (queryParameters.size() != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }

        // Not a Map
        if (!(queryParameters.get(0) instanceof Map)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid argument type, expecting Map");
        }
    }

    private void validateMapQueryBetween(String queryPartDescription) {
        // Other than 2 arguments
        if (queryParameters.size() != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two");
        }

        Object value = queryParameters.get(0);
        // Not a Map
        if (!(value instanceof Map)) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid argument type, expecting Map");
        }

        // Arguments of different classes
        if (!value.getClass().equals(queryParameters.get(1).getClass())) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid arguments type, expecting both " +
                "to be of the same class");
        }
    }

    @Override
    public Qualifier process() {
        Qualifier qualifier;
        QualifierBuilder qb = Qualifier.builder();
        int paramsSize = queryParameters.size();

        if (filterOperation == BETWEEN || filterOperation == IN || filterOperation == NOT_IN) {
            setQualifierBuilderValue(qb, queryParameters.get(0));
            if (queryParameters.size() == 2) setQualifierBuilderSecondValue(qb, queryParameters.get(1));
            qualifier = setQualifier(qb, fieldName, filterOperation, part, null);
            return qualifier;
        }

        if (paramsSize == 2) {
            qualifier = processMapTwoParams(part, queryParameters, filterOperation, fieldName);
        } else if (queryParameters.size() < 2) {
            setQualifierBuilderValue(qb, queryParameters.get(0));
            qualifier = setQualifier(qb, fieldName, filterOperation, part, null);
        } else { // multiple parameters
            qualifier = processMapMultipleParams(part, queryParameters, filterOperation, fieldName);
        }

        return qualifier;
    }

    private Qualifier processMapTwoParams(Part part, List<Object> params, FilterOperation op, String fieldName) {
        Qualifier qualifier;
        if (op == FilterOperation.CONTAINING) {
            qualifier = processMapContaining(part, fieldName, MAP_KEYS_CONTAIN, MAP_VALUES_CONTAIN,
                MAP_VAL_EQ_BY_KEY);
        } else if (op == FilterOperation.NOT_CONTAINING) {
            qualifier = processMapContaining(part, fieldName, MAP_KEYS_NOT_CONTAIN, MAP_VALUES_NOT_CONTAIN,
                MAP_VAL_NOTEQ_BY_KEY);
        } else {
            qualifier = processMapOtherThanContaining(part, params, op, fieldName);
        }

        return qualifier;
    }

    private Qualifier processMapContaining(Part part, String fieldName, FilterOperation keysOp,
                                           FilterOperation valuesOp, FilterOperation byKeyOp) {
        FilterOperation op = byKeyOp;
        QualifierBuilder qb = Qualifier.builder();

        if (queryParameters.get(0) instanceof AerospikeQueryCriterion queryCriterion) {
            switch (queryCriterion) {
                case KEY -> {
                    op = keysOp;
                    setQualifierBuilderValue(qb, queryParameters.get(1));
                }
                case VALUE -> {
                    op = valuesOp;
                    setQualifierBuilderValue(qb, queryParameters.get(1));
                }
                default -> throw new UnsupportedOperationException("Unsupported parameter: " + queryCriterion);
            }
        }
        return setQualifier(qb, fieldName, op, part, null);
    }

    private Qualifier processMapOtherThanContaining(Part part, List<Object> queryParameters, FilterOperation op,
                                                    String fieldName) {
        QualifierBuilder qb = Qualifier.builder();
        Object param1 = queryParameters.get(0);
        List<String> dotPath = List.of(part.getProperty().toDotPath(), Value.get(param1).toString());

        if (queryParameters.size() == 3) {
            op = getCorrespondingMapValueFilterOperationOrFail(op);
        }

        setQualifierBuilderKey(qb, queryParameters.get(0));
        setQualifierBuilderValue(qb, queryParameters.get(1));
        return setQualifier(qb, fieldName, op, part, dotPath);
    }

    private Qualifier processMapMultipleParams(Part part, List<Object> params, FilterOperation op, String fieldName) {
        if (op == FilterOperation.CONTAINING || op == FilterOperation.NOT_CONTAINING) {
            return processMapMultipleParamsContaining(part, params, op, fieldName);
        } else {
            return processMapOtherThanContaining(part, params, op, fieldName);
        }
    }

    private Qualifier processMapMultipleParamsContaining(Part part, List<Object> params, FilterOperation op,
                                                         String fieldName) {
        List<String> dotPath;
        QualifierBuilder qb = Qualifier.builder();
        AerospikeQueryCriterion queryCriterion;
        Object firstParam = params.get(0);

        if (firstParam instanceof AerospikeQueryCriterion) {
            queryCriterion = (AerospikeQueryCriterion) params.get(0);
            if (queryCriterion == KEY_VALUE_PAIR) {
                switch (op) {
                    case EQ, CONTAINING -> op = MAP_VAL_EQ_BY_KEY;
                    case NOTEQ -> op = MAP_VAL_NOTEQ_BY_KEY;
                    case NOT_CONTAINING -> op = MAP_VAL_NOT_CONTAINING_BY_KEY;
                }
                Value key = getValueOfQueryParameter(params.get(1));
                qb.setKey(key);
                dotPath = List.of(part.getProperty().toDotPath(), key.toString());
                setQualifierBuilderValue(qb, queryParameters.get(2));
            } else {
                throw new UnsupportedOperationException("Unsupported parameter: " + queryCriterion);
            }
        } else {
            throw new UnsupportedOperationException(
                String.format("Unsupported combination of operation %s and the first parameter %s", op, firstParam));
        }

        if (op == MAP_VAL_CONTAINING_BY_KEY || op == MAP_VAL_NOT_CONTAINING_BY_KEY
            || op == MAP_VAL_EQ_BY_KEY || op == MAP_VAL_NOTEQ_BY_KEY) {
            return setQualifier(qb, fieldName, op, part, dotPath);
        } else {
            return qualifierAndConcatenated(converter, params, qb, part, fieldName, op, dotPath);
        }
    }
}
