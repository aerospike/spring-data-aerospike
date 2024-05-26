package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.command.ParticleType;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.aerospike.query.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.FilterOperation.CONTAINING;
import static org.springframework.data.aerospike.query.FilterOperation.IN;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NOT_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.NOT_CONTAINING;
import static org.springframework.data.aerospike.query.FilterOperation.NOT_IN;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getCollectionElementsClass;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getCorrespondingMapValueFilterOperationOrFail;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderKey;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderSecondValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateQueryIn;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateQueryIsNull;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateTypes;

public class CollectionQueryCreator implements IAerospikeQueryCreator {

    private final Part part;
    private final PropertyPath propertyPath;
    private final AerospikePersistentProperty property;
    private final String fieldName;
    private final List<Object> queryParameters;
    private final FilterOperation filterOperation;
    private final MappingAerospikeConverter converter;
    private final boolean isNested;
    private final ServerVersionSupport versionSupport;

    public CollectionQueryCreator(Part part, PropertyPath propertyPath, AerospikePersistentProperty property,
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
            case CONTAINING, NOT_CONTAINING -> validateCollectionQueryContaining(queryParameters, queryPartDescription);
            case EQ, NOTEQ, GT, GTEQ, LT, LTEQ -> validateCollectionQueryComparison(queryParameters,
                queryPartDescription);
            case BETWEEN -> validateCollectionQueryBetween(queryParameters, queryPartDescription);
            case IN, NOT_IN -> validateQueryIn(queryParameters, queryPartDescription);
            case IS_NOT_NULL, IS_NULL -> validateQueryIsNull(queryParameters, queryPartDescription);
            default -> throw new UnsupportedOperationException("Unsupported operation: " + queryPartDescription);
        }
    }

    private void validateCollectionQueryContaining(List<Object> queryParameters, String queryPartDescription) {
        // Other than one argument
        if (queryParameters.size() != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }

        validateCollectionContainingTypes(part.getProperty(), queryParameters, queryPartDescription);
    }

    private void validateCollectionQueryComparison(List<Object> queryParameters, String queryPartDescription) {
        // Other than 1 argument
        if (queryParameters.size() != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }

        if (queryParameters.get(0) instanceof Collection) {
            validateTypes(converter, Collection.class, queryParameters, this.filterOperation, queryPartDescription);
        } else {
            throw new IllegalArgumentException(queryPartDescription + ": invalid argument type, expecting Collection");
        }
    }

    private void validateCollectionQueryBetween(List<Object> queryParameters, String queryPartDescription) {
        // No arguments
        if (queryParameters.size() != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two");
        }

        // Not Collection
        Object value = queryParameters.get(0);
        if (value instanceof Collection) {
            validateTypes(converter, Collection.class, queryParameters, filterOperation, queryPartDescription);
        } else {
            throw new IllegalArgumentException(queryPartDescription + ": invalid argument type, expecting Collection");
        }

        // Arguments of different classes
        if (!value.getClass().equals(queryParameters.get(1).getClass())) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid arguments type, expecting both " +
                "to be of the same class");
        }
    }

    private void validateCollectionContainingTypes(PropertyPath property, List<Object> queryParameters,
                                                   String queryPartDescription) {
        Object value = queryParameters.get(0);
        if (value instanceof Collection) {
            validateTypes(converter, Collection.class, queryParameters, filterOperation, queryPartDescription);
        } else if (!(value instanceof CriteriaDefinition.AerospikeNullQueryCriterion)) {
            // Not null param
            // Determining class of Collection's elements
            Class<?> componentsClass = getCollectionElementsClass(property);
            if (componentsClass != null) {
                validateTypes(converter, componentsClass, queryParameters, filterOperation, queryPartDescription,
                    "Collection");
            }
        }
    }

    @Override
    public Qualifier process() {
        QueryQualifierBuilder qb = new QueryQualifierBuilder();
        FilterOperation op = filterOperation;

        if (filterOperation == BETWEEN || filterOperation == IN || filterOperation == NOT_IN) {
            setQualifierBuilderValue(qb, queryParameters.get(0));
            if (queryParameters.size() == 2) setQualifierBuilderSecondValue(qb, queryParameters.get(1));
        }

        List<String> dotPath = null;
        if (isNested) { // POJO field
            if (op == CONTAINING || op == NOT_CONTAINING) {
                qb.setNestedType(ParticleType.LIST);
            }

            // getting MAP_VAL_ operation because the property is in a POJO which is represented by a Map in DB
            op = getCorrespondingMapValueFilterOperationOrFail(op);
            dotPath = List.of(part.getProperty().toDotPath());
        } else { // first level
            if (op == CONTAINING || op == NOT_CONTAINING) {
                op = getCorrespondingListFilterOperationOrFail(op);
            }
        }

        if (queryParameters.isEmpty() && (filterOperation == IS_NOT_NULL || filterOperation == IS_NULL)) {
            setQualifierBuilderValue(qb, property.getFieldName());
        } else {
            setQualifierBuilderValue(qb, queryParameters.get(0));
            if (isNested) {
                setQualifierBuilderKey(qb, property.getFieldName());
            }
        }

        return setQualifier(qb, fieldName, op, part, dotPath, versionSupport);
    }

    private FilterOperation getCorrespondingListFilterOperationOrFail(FilterOperation op) {
        try {
            return FilterOperation.valueOf("COLLECTION_VAL_" + op);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot find corresponding COLLECTION_VAL_... FilterOperation for '" + op + "'");
        }
    }
}
