package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.aerospike.query.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NOT_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NULL;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getCorrespondingMapValueFilterOperationOrFail;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderKey;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderSecondValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateTypes;

public class SimplePropertyQueryCreator implements IAerospikeQueryCreator {

    private final Part part;
    private final PropertyPath propertyPath;
    private final AerospikePersistentProperty property;
    private final String fieldName;
    private final List<Object> queryParameters;
    private final FilterOperation filterOperation;
    private final MappingAerospikeConverter converter;
    private final boolean isBooleanQuery;

    public SimplePropertyQueryCreator(Part part, PropertyPath propertyPath, AerospikePersistentProperty property,
                                      String fieldName, List<Object> queryParameters,
                                      FilterOperation filterOperation, MappingAerospikeConverter converter) {
        this.part = part;
        this.isBooleanQuery = part.getType() == Part.Type.FALSE || part.getType() == Part.Type.TRUE;
        this.propertyPath = propertyPath;
        this.property = property;
        this.fieldName = fieldName;
        this.queryParameters = queryParameters;
        this.filterOperation = filterOperation;
        this.converter = converter;
    }

    @Override
    public void validate() {
        String queryPartDescription = String.join(" ", propertyPath.toString(), filterOperation.toString());
        switch (filterOperation) {
            case CONTAINING, NOT_CONTAINING, GT, GTEQ, LT, LTEQ, LIKE, STARTS_WITH, ENDS_WITH, EQ, NOTEQ -> {
                validateSimplePropertyQueryComparison(queryPartDescription, queryParameters);
                validateTypes(converter, propertyPath, queryParameters, queryPartDescription);
            }
            case IN, NOT_IN -> {
                validateSimplePropertyQueryComparison(queryPartDescription, queryParameters);
                validateSimplePropertyInQueryTypes(queryPartDescription, queryParameters);
            }
            case BETWEEN -> {
                validateSimplePropertyQueryBetween(queryPartDescription, queryParameters);
                validateTypes(converter, propertyPath, queryParameters, queryPartDescription);
            }
            case IS_NOT_NULL, IS_NULL -> {
                validateSimplePropertyQueryIsNull(queryPartDescription, queryParameters);
                validateTypes(converter, propertyPath, queryParameters, queryPartDescription);
            }
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", filterOperation, property));
        }
    }

    private void validateSimplePropertyQueryComparison(String queryPartDescription, List<Object> queryParameters) {
        // Number of arguments is not one
        int paramsSize = queryParameters.size();
        if (paramsSize != 1 && !isBooleanQuery) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }

        if (isBooleanQuery && paramsSize != 0) {
            throw new IllegalArgumentException(queryPartDescription + ": no arguments expected");
        }
    }

    private void validateSimplePropertyInQueryTypes(String queryPartDescription, List<Object> queryParameters) {
        Object param1 = queryParameters.get(0);
        if (param1 instanceof Collection) {
            validateTypes(converter, Collection.class, queryParameters, queryPartDescription);
        }
    }

    private void validateSimplePropertyQueryBetween(String queryPartDescription, List<Object> queryParameters) {
        // Number of arguments is not two
        if (queryParameters.size() != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two");
        }
    }

    private void validateSimplePropertyQueryIsNull(String queryPartDescription, List<Object> queryParameters) {
        // Number of arguments is not zero
        if (queryParameters.size() != 0) {
            throw new IllegalArgumentException(queryPartDescription + ": expecting no arguments");
        }
    }

    @Override
    public Qualifier process() {
        List<String> dotPath = null;
//        if (value2 == null && parametersIterator.hasNext()) {
//            value2 = convertIfNecessary(parametersIterator.next());
//        }

//        Object value1 = queryParameters.get(0);
//        Object value2 = null;
//        Object value3 = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();
        FilterOperation op = filterOperation;

        if (isBooleanQuery) {
            // setting the value (false or true) for a boolean query without arguments
            queryParameters.add(Value.get(convertPartTypeToBoolean(part.getType())));
        }

//        if (filterOperation == BETWEEN || filterOperation == IN || filterOperation == NOT_IN) {
        if (filterOperation == BETWEEN) {
//            setQualifierBuilderValue(qb, queryParameters.get(0));
            if (queryParameters.size() >=2) setQualifierBuilderSecondValue(qb, queryParameters.get(1));
        }

        if (part.getProperty().hasNext()) { // if it is a POJO field // TODO: convert to a flag and pass it
//            PropertyPath nestedProperty = getNestedPropertyPath(part.getProperty());

            if (filterOperation == FilterOperation.BETWEEN) {
//                value3 = Value.get(queryParameters.get(1)); // contains upper limit
//                queryParameters.add(Value.get(property.getFieldName())); // setting the upper bound
            } else if (filterOperation == IS_NOT_NULL || filterOperation == IS_NULL) {
//                value1 = Value.get(property.getFieldName()); // contains key (field name)
            }

            // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
            op = getCorrespondingMapValueFilterOperationOrFail(op);
//            queryParameters.add(0, Value.get(property.getFieldName())); // setting the key
            if (queryParameters.size() >= 1) setQualifierBuilderValue(qb, queryParameters.get(0));
            setQualifierBuilderKey(qb, property.getFieldName());
            dotPath = List.of(part.getProperty().toDotPath());
        } else {
            setQualifierBuilderValue(qb, queryParameters.get(0));
//            setQualifierBuilderKey(qb, property.getFieldName());
        }

        return setQualifier(converter, qb, fieldName, op, part, dotPath);
    }

    private boolean convertPartTypeToBoolean(Part.Type type) {
        if (type == Part.Type.FALSE) {
            return false;
        } else if (type == Part.Type.TRUE) {
            return true;
        } else {
            throw new IllegalStateException("Unexpected Part.Type: not boolean");
        }
    }
}
