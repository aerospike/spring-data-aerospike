package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;

import java.util.List;

import static org.springframework.data.aerospike.query.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.FilterOperation.IN;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NOT_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.NOT_IN;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getCorrespondingMapValueFilterOperationOrFail;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderKey;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderSecondValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateTypes;

public class PojoQueryCreator implements IAerospikeQueryCreator {

    private final Part part;
    private final AerospikePersistentProperty property;
    private final PropertyPath propertyPath;
    private final String fieldName;
    private final List<Object> queryParameters;
    private final FilterOperation filterOperation;
    private final MappingAerospikeConverter converter;
    private final boolean isNested;

    public PojoQueryCreator(Part part, PropertyPath propertyPath, AerospikePersistentProperty property,
                            String fieldName, List<Object> queryParameters, FilterOperation filterOperation,
                            MappingAerospikeConverter converter, boolean isNested) {
        this.part = part;
        this.propertyPath = propertyPath;
        this.property = property;
        this.fieldName = fieldName;
        this.queryParameters = queryParameters;
        this.filterOperation = filterOperation;
        this.converter = converter;
        this.isNested = isNested;
    }

    @Override
    public void validate() {
        String queryPartDescription = String.join(" ", propertyPath.toString(), filterOperation.toString());
        switch (filterOperation) {
            case CONTAINING, NOT_CONTAINING -> throw new UnsupportedOperationException("Unsupported operation, " +
                "please use queries like 'findByPojoField()' directly addressing the required fields");
            case EQ, NOTEQ, GT, GTEQ, LT, LTEQ -> validatePojoQueryComparison(queryParameters,
                queryPartDescription);
            case BETWEEN -> validatePojoQueryBetween(queryParameters, queryPartDescription);
            case IN, NOT_IN -> validatePojoQueryIn(queryParameters, queryPartDescription);
            case IS_NOT_NULL, IS_NULL -> validatePojoQueryIsNull(queryParameters, queryPartDescription);
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", filterOperation, property));
        }

        validateTypes(converter, propertyPath, filterOperation, queryParameters);
    }

    private void validatePojoQueryComparison(List<Object> queryParameters, String queryPartDescription) {
        // Number of arguments is not two
        if (queryParameters.size() != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one " +
                "POJO");
        }
    }

    private void validatePojoQueryBetween(List<Object> queryParameters, String queryPartDescription) {
        // Number of arguments is not two
        if (queryParameters.size() != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two " +
                "POJOs");
        }
    }

    private void validatePojoQueryIn(List<Object> queryParameters, String queryPartDescription) {
        // Number of arguments is not one
        if (queryParameters.size() != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }
    }

    private void validatePojoQueryIsNull(List<Object> queryParameters, String queryPartDescription) {
        // Number of arguments is not zero
        if (!queryParameters.isEmpty()) {
            throw new IllegalArgumentException(queryPartDescription + ": expecting no arguments");
        }
    }

    @Override
    public Qualifier process() {
        List<String> dotPath = null;
//        if (value2 == null && parametersIterator.hasNext()) {
//            value2 = convertIfNecessary(parametersIterator.next());
//        }

        Qualifier.QualifierBuilder qb = Qualifier.builder();
        FilterOperation op = filterOperation;

        if (filterOperation == BETWEEN || filterOperation == IN || filterOperation == NOT_IN) {
            setQualifierBuilderValue(qb, queryParameters.get(0));
            if (queryParameters.size() >=2) setQualifierBuilderSecondValue(qb, queryParameters.get(1));
        }

        if (isNested) { // if it is a POJO field
//            PropertyPath nestedProperty = getNestedPropertyPath(part.getProperty());

            if (filterOperation == FilterOperation.BETWEEN) {
//                value3 = Value.get(queryParameters.get(1)); // contains upper limit
            } else if (filterOperation == IS_NOT_NULL || filterOperation == IS_NULL) {
//                value1 = Value.get(property.getFieldName()); // contains key (field name)
            }

            // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
            op = getCorrespondingMapValueFilterOperationOrFail(filterOperation);
//            queryParameters.add(0, Value.get(property.getFieldName())); // setting the key (field name)
            setQualifierBuilderKey(qb, property.getFieldName());
            setQualifierBuilderValue(qb, queryParameters.get(0));
            dotPath = List.of(part.getProperty().toDotPath());
        } else { // if it is a first level POJO
            if (op != FilterOperation.BETWEEN) {
                // if it is a POJO compared for equality it already has FilterOperation
//                queryParameters.add(0, Value.get(property.getFieldName())); // setting the key (field name)
//                queryParameters.add(1, Value.get(property.getFieldName())); // setting the key (field name)
                if (queryParameters.size() >= 1) setQualifierBuilderValue(qb, queryParameters.get(0));
            }
        }

        return setQualifier(converter, qb, fieldName, op, part, dotPath);
    }
}
