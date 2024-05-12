package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.query.qualifier.QualifierBuilder;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.util.TypeInformation;

import java.util.List;
import java.util.Map;

import static org.springframework.data.aerospike.query.FilterOperation.*;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.*;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeNullQueryCriterion.NULL_PARAM;
import static org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeQueryCriterion.KEY_VALUE_PAIR;

public class MapQueryCreator implements IAerospikeQueryCreator {

    private final Part part;
    private final AerospikePersistentProperty property;
    private final String fieldName;
    private final List<Object> queryParameters;
    private final FilterOperation filterOperation;
    private final MappingAerospikeConverter converter;
    private final ServerVersionSupport versionSupport;
    private final boolean isNested;

    public MapQueryCreator(Part part, AerospikePersistentProperty property, String fieldName,
                           List<Object> queryParameters, FilterOperation filterOperation,
                           MappingAerospikeConverter converter, ServerVersionSupport versionSupport, boolean isNested) {
        this.part = part;
        this.property = property;
        this.fieldName = fieldName;
        this.queryParameters = queryParameters;
        this.filterOperation = filterOperation;
        this.converter = converter;
        this.isNested = isNested;
        this.versionSupport = versionSupport;
    }

    @Override
    public void validate() {
        String queryPartDescription = String.join(" ", part.getProperty().toString(), filterOperation.toString());
        switch (filterOperation) {
            case CONTAINING, NOT_CONTAINING -> validateMapQueryContaining(queryPartDescription);
            case EQ, NOTEQ, GT, GTEQ, LT, LTEQ -> validateMapQueryComparison(queryPartDescription);
            case BETWEEN -> validateMapQueryBetween(queryPartDescription);
            case IN, NOT_IN -> validateQueryIn(queryParameters, queryPartDescription);
            case IS_NOT_NULL, IS_NULL -> validateQueryIsNull(queryParameters, queryPartDescription);
            default -> throw new UnsupportedOperationException("Unsupported operation: " + queryPartDescription);
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
        Qualifier qualifier = null;
        QualifierBuilder qb = Qualifier.builder();
        int paramsSize = queryParameters.size();
        List<String> dotPath = null;
        FilterOperation op = filterOperation;

        if (filterOperation == BETWEEN || filterOperation == IN || filterOperation == NOT_IN) {
            setQualifierBuilderValue(qb, queryParameters.get(0));
            if (queryParameters.size() == 2) setQualifierBuilderSecondValue(qb, queryParameters.get(1));
            if (isNested) {
                setQualifierBuilderKey(qb, property.getFieldName());
                dotPath = List.of(part.getProperty().toDotPath());
                // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
                op = getCorrespondingMapValueFilterOperationOrFail(filterOperation);
            }
            qualifier = setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
            return qualifier;
        }

        // POJO field
        if (isNested && (op == CONTAINING || op == NOT_CONTAINING)) {
            // for nested MapContaining queries
            qb.setNestedType(ParticleType.MAP);
        }

        if (queryParameters.size() < 2) {
            if (queryParameters.isEmpty() && (filterOperation == IS_NOT_NULL || filterOperation == IS_NULL)) {
                setQualifierBuilderValue(qb, property.getFieldName());
            } else {
                setQualifierBuilderValue(qb, queryParameters.get(0));
                if (isNested) {
                    setQualifierBuilderKey(qb, property.getFieldName());
                }
            }
            if (isNested) {
                // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
                op = getCorrespondingMapValueFilterOperationOrFail(filterOperation);
                dotPath = List.of(part.getProperty().toDotPath());
            }
            qualifier = setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
        }

        if (paramsSize == 2) {
            if (isNested) {
                setQualifierBuilderKey(qb, property.getFieldName());
            }
            qualifier = processMapTwoParams(qb, part, queryParameters, filterOperation, fieldName);
        }

        if (queryParameters.size() > 2) { // multiple parameters
            qualifier = processMapMultipleParams(qb);
        }

        return qualifier;
    }

    private Qualifier processMapTwoParams(QualifierBuilder qb, Part part, List<Object> params, FilterOperation op,
                                          String fieldName) {
        Qualifier qualifier;
        if (op == FilterOperation.CONTAINING) {
            qualifier = processMapContaining(qb, part, fieldName, MAP_KEYS_CONTAIN, MAP_VALUES_CONTAIN,
                MAP_VAL_EQ_BY_KEY);
        } else if (op == FilterOperation.NOT_CONTAINING) {
            qualifier = processMapContaining(qb, part, fieldName, MAP_KEYS_NOT_CONTAIN, MAP_VALUES_NOT_CONTAIN,
                MAP_VAL_NOTEQ_BY_KEY);
        } else {
            qualifier = processMapOtherThanContaining(qb, part, params, op, fieldName);
        }

        return qualifier;
    }

    private Qualifier processMapContaining(QualifierBuilder qb, Part part, String fieldName, FilterOperation keysOp,
                                           FilterOperation valuesOp, FilterOperation byKeyOp) {
        FilterOperation op = byKeyOp;
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
        return setQualifier(qb, fieldName, op, part, null, versionSupport);
    }

    private Qualifier processMapOtherThanContaining(QualifierBuilder qb, Part part, List<Object> queryParameters,
                                                    FilterOperation op,
                                                    String fieldName) {
        Object param1 = queryParameters.get(0);
        List<String> dotPath = List.of(part.getProperty().toDotPath(), Value.get(param1).toString());

        if (queryParameters.size() == 3) {
            op = getCorrespondingMapValueFilterOperationOrFail(op);
        }

        setQualifierBuilderKey(qb, queryParameters.get(0));
        setQualifierBuilderValue(qb, queryParameters.get(1));
        return setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
    }

    private Qualifier processMapMultipleParams(QualifierBuilder qb) {
        if (filterOperation == FilterOperation.CONTAINING || filterOperation == FilterOperation.NOT_CONTAINING) {
            return processMapMultipleParamsContaining(qb, part, queryParameters, filterOperation, fieldName, isNested);
        } else {
            return processMapOtherThanContaining(qb, part, queryParameters, filterOperation, fieldName);
        }
    }

    private Qualifier processMapMultipleParamsContaining(QualifierBuilder qb, Part part, List<Object> params,
                                                         FilterOperation op, String fieldName, boolean isNested) {
        List<String> dotPath;
        AerospikeQueryCriterion queryCriterion;
        Object firstParam = params.get(0);

        if (firstParam instanceof AerospikeQueryCriterion) {
            queryCriterion = (AerospikeQueryCriterion) firstParam;
            if (queryCriterion == KEY_VALUE_PAIR) {
                Value key;
                if (isNested) {
                    switch (op) {
                        case EQ, CONTAINING -> op = MAP_VAL_CONTAINING_BY_KEY;
                        case NOTEQ, NOT_CONTAINING -> op = MAP_VAL_NOT_CONTAINING_BY_KEY;
                    }
                    key = getValueOfQueryParameter(property.getFieldName());
                    setQualifierBuilderSecondKey(qb, params.get(1));
                } else {
                    switch (op) {
                        case EQ, CONTAINING -> op = MAP_VAL_EQ_BY_KEY;
                        case NOTEQ, NOT_CONTAINING -> op = MAP_VAL_NOTEQ_BY_KEY;
                    }
                    key = getValueOfQueryParameter(params.get(1));
                }
                qb.setKey(key);
                dotPath = List.of(part.getProperty().toDotPath(), key.toString());
                setQualifierBuilderValue(qb, params.get(2));
            } else {
                throw new UnsupportedOperationException("Unsupported parameter: " + queryCriterion);
            }
        } else {
            throw new UnsupportedOperationException(
                String.format("Unsupported combination of operation %s and the first parameter %s", op, firstParam));
        }

        if (op == MAP_VAL_CONTAINING_BY_KEY || op == MAP_VAL_NOT_CONTAINING_BY_KEY
            || op == MAP_VAL_EQ_BY_KEY || op == MAP_VAL_NOTEQ_BY_KEY) {
            return setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
        } else {
            return qualifierAndConcatenated(versionSupport, params, qb, part, fieldName, op, dotPath);
        }
    }
}
