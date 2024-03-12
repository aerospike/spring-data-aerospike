package org.springframework.data.aerospike.repository.query;

import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.parser.Part;

import java.util.Collection;
import java.util.List;

import static org.springframework.data.aerospike.query.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.FilterOperation.IN;
import static org.springframework.data.aerospike.query.FilterOperation.LIST_VAL_CONTAINING;
import static org.springframework.data.aerospike.query.FilterOperation.NOT_IN;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getElementsClass;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.qualifierAndConcatenated;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderSecondValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.setQualifierBuilderValue;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.validateTypes;

public class CollectionQueryCreator implements IAerospikeQueryCreator {

    private final Part part;
    private final AerospikePersistentProperty property;
    private final String fieldName;
    private final List<Object> queryParameters;
    private final FilterOperation filterOperation;
    private final MappingAerospikeConverter converter;

    public CollectionQueryCreator(Part part, AerospikePersistentProperty property, String fieldName,
                                  List<Object> queryParameters, FilterOperation filterOperation, MappingAerospikeConverter converter) {
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
            case CONTAINING, NOT_CONTAINING -> validateCollectionQueryContaining(queryParameters, queryPartDescription);
            case EQ, NOTEQ, GT, GTEQ, LT, LTEQ -> validateCollectionQueryComparison(queryParameters,
                queryPartDescription);
            case BETWEEN -> validateCollectionQueryBetween(queryParameters, queryPartDescription);
            default -> throw new UnsupportedOperationException(
                String.format("Unsupported operation: %s applied to %s", filterOperation, property));
        }

        validateCollectionQueryTypes(part.getProperty(), queryParameters, queryPartDescription);
    }

    private void validateCollectionQueryContaining(List<Object> queryParameters, String queryPartDescription) {
        // No arguments
        if (queryParameters.size() == 0) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting at " +
                "least one");
        }
    }

    private void validateCollectionQueryComparison(List<Object> queryParameters, String queryPartDescription) {
        // Other than 1 argument
        if (queryParameters.size() != 1) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting one");
        }
    }

    private void validateCollectionQueryBetween(List<Object> queryParameters, String queryPartDescription) {
        // No arguments
        if (queryParameters.size() != 2) {
            throw new IllegalArgumentException(queryPartDescription + ": invalid number of arguments, expecting two");
        }
    }

    private void validateCollectionQueryTypes(PropertyPath property, List<Object> queryParameters,
                                              String queryPartDescription) {
        Object value1 = queryParameters.get(0);
        if (value1 instanceof Collection) {
            validateTypes(converter, Collection.class, queryParameters, queryPartDescription);
        } else if (value1 instanceof CriteriaDefinition.AerospikeNullQueryCriterion) {
            // Not more than one null value
            if (queryParameters.size() > 1) { // TODO: check logic
                throw new IllegalArgumentException(String.format("%s: invalid number of null arguments, expecting " +
                    "one", queryPartDescription));
            }
        } else {
            // Determining class of Collection's elements
            Class<?> componentsClass = getElementsClass(property);
            if (componentsClass != null) {
                validateTypes(converter, componentsClass, queryParameters, queryPartDescription, "Collection");
            }
        }
    }

    @Override
    public Qualifier process() {
        Qualifier.QualifierBuilder qb = Qualifier.builder();
//        Object value1 = queryParameters.get(0);
//        Object value2 = queryParameters.get(1);
        FilterOperation op = filterOperation;

        if (filterOperation == BETWEEN || filterOperation == IN || filterOperation == NOT_IN) {
            setQualifierBuilderValue(qb, queryParameters.get(0));
            if (queryParameters.size() >=2) setQualifierBuilderSecondValue(qb, queryParameters.get(1));
        }

        if (queryParameters.size() > 2) {
            if (filterOperation == FilterOperation.CONTAINING) {
                op = LIST_VAL_CONTAINING;
//                params.add(0, value1); // value1 stores the first parameter
                return qualifierAndConcatenated(converter, queryParameters, qb, part, fieldName, op, null,
                    queryParameters);
            }
        } else if (!(queryParameters.get(0) instanceof Collection<?>)) {
            op = getCorrespondingListFilterOperationOrFail(op);
            setQualifierBuilderValue(qb, queryParameters.get(0));
        } else {
            setQualifierBuilderValue(qb, queryParameters.get(0));
        }

        return setQualifier(converter, qb, fieldName, op, part, null);
    }

    private FilterOperation getCorrespondingListFilterOperationOrFail(FilterOperation op) {
        try {
            return FilterOperation.valueOf("LIST_VAL_" + op);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot find corresponding LIST_VAL_... FilterOperation for '" + op + "'");
        }
    }
}
