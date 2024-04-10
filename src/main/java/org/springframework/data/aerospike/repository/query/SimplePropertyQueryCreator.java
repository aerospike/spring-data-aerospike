package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.query.qualifier.QualifierBuilder;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.aerospike.query.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.FilterOperation.CONTAINING;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NOT_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.NOT_CONTAINING;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getCorrespondingMapValueFilterOperationOrFail;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderKey;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderSecondValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateQueryIsNull;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateTypes;

public class SimplePropertyQueryCreator implements IAerospikeQueryCreator {

    private final Part part;
    private final PropertyPath propertyPath;
    private final AerospikePersistentProperty property;
    private final String fieldName;
    private final List<Object> queryParameters;
    private final FilterOperation filterOperation;
    private final MappingAerospikeConverter converter;
    private final boolean isNested;
    private final boolean isBooleanQuery;

    public SimplePropertyQueryCreator(Part part, PropertyPath propertyPath, AerospikePersistentProperty property,
                                      String fieldName, List<Object> queryParameters,
                                      FilterOperation filterOperation, MappingAerospikeConverter converter,
                                      boolean isNested) {
        this.part = part;
        this.isBooleanQuery = part.getType() == Part.Type.FALSE || part.getType() == Part.Type.TRUE;
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
            case CONTAINING, NOT_CONTAINING, GT, GTEQ, LT, LTEQ, LIKE, STARTS_WITH, ENDS_WITH, EQ, NOTEQ -> {
                validateSimplePropertyQueryComparison(queryPartDescription, queryParameters);
                validateSimplePropertyContaining(queryPartDescription, queryParameters, filterOperation, propertyPath);
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
                validateQueryIsNull(queryParameters, queryPartDescription);
                validateTypes(converter, propertyPath, queryParameters, queryPartDescription);
            }
            default -> throw new UnsupportedOperationException("Unsupported operation: " + queryPartDescription);
        }
    }

    private void validateSimplePropertyContaining(String queryPartDescription, List<Object> queryParameters,
                                                  FilterOperation filterOperation, PropertyPath propertyPath) {
        if ((filterOperation == CONTAINING || filterOperation == NOT_CONTAINING)
            && (!propertyPath.getType().equals(String.class) || !(queryParameters.get(0) instanceof String))) {
            throw new IllegalArgumentException(queryPartDescription + ": expecting both property and argument to be " +
                "a String");
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

    @Override
    public Qualifier process() {
        QualifierBuilder qb = Qualifier.builder();

        if (isBooleanQuery) {
            // setting the value for a boolean query without arguments
            queryParameters.add(Value.get(convertPartTypeToBoolean(part.getType())));
        }

        if (filterOperation == BETWEEN) {
            // the correct number of arguments is validated before
            setQualifierBuilderSecondValue(qb, queryParameters.get(1));
        }

        if (filterOperation == CONTAINING || filterOperation == NOT_CONTAINING) {
            // only a String can be used with CONTAINING, it is validated in validateSimplePropertyContaining()
            qb.setFieldType(ParticleType.STRING);
        }

        List<String> dotPath = null;
        FilterOperation op = filterOperation;
        if (isNested) { // POJO field
            // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
            op = getCorrespondingMapValueFilterOperationOrFail(op);

            if (queryParameters.isEmpty() && (filterOperation == IS_NOT_NULL || filterOperation == IS_NULL)) {
                setQualifierBuilderValue(qb, property.getFieldName());
            } else {
                setQualifierBuilderValue(qb, queryParameters.get(0));
                setQualifierBuilderKey(qb, property.getFieldName());
            }
            dotPath = List.of(part.getProperty().toDotPath());
        } else { // first level simple property
            setQualifierBuilderValue(qb, queryParameters.get(0));
        }

        return setQualifier(qb, fieldName, op, part, dotPath);
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
