package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.core.PropertyPath;
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
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateQueryIn;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateQueryIsNull;
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
    private ServerVersionSupport versionSupport;

    public PojoQueryCreator(Part part, PropertyPath propertyPath, AerospikePersistentProperty property,
                            String fieldName, List<Object> queryParameters, FilterOperation filterOperation,
                            MappingAerospikeConverter converter, boolean isNested,
                            ServerVersionSupport versionSupport) {
        this.part = part;
        this.propertyPath = propertyPath;
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
        String queryPartDescription = String.join(" ", propertyPath.toString(), filterOperation.toString());
        switch (filterOperation) {
            case CONTAINING, NOT_CONTAINING -> throw new UnsupportedOperationException("Unsupported operation, " +
                "please use queries like 'findByPojoField()' directly addressing the required fields");
            case EQ, NOTEQ, GT, GTEQ, LT, LTEQ -> validatePojoQueryComparison(queryParameters,
                queryPartDescription);
            case BETWEEN -> validatePojoQueryBetween(queryParameters, queryPartDescription);
            case IN, NOT_IN -> validateQueryIn(queryParameters, queryPartDescription);
            case IS_NOT_NULL, IS_NULL -> validateQueryIsNull(queryParameters, queryPartDescription);
            default -> throw new UnsupportedOperationException("Unsupported operation: " + queryPartDescription);
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

    @Override
    public Qualifier process() {
        Qualifier qualifier;
        QueryQualifierBuilder qb = new QueryQualifierBuilder();
        FilterOperation op = filterOperation;
        List<String> dotPath = null;


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

        if (isNested) { // POJO field
            // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
            op = getCorrespondingMapValueFilterOperationOrFail(filterOperation);
            if (queryParameters.isEmpty() && (filterOperation == IS_NOT_NULL || filterOperation == IS_NULL)) {
                setQualifierBuilderValue(qb, property.getFieldName());
            } else {
                setQualifierBuilderValue(qb, queryParameters.get(0));
                setQualifierBuilderKey(qb, property.getFieldName());
            }
            dotPath = List.of(part.getProperty().toDotPath());
        } else { // first level POJO
            if (op != FilterOperation.BETWEEN) {
                if (!queryParameters.isEmpty()) setQualifierBuilderValue(qb, queryParameters.get(0));
            }
        }

        return setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
    }
}
