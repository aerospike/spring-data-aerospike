package org.springframework.data.aerospike.query.qualifier;

import org.springframework.data.aerospike.annotation.Beta;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.util.Assert;

import java.util.Collection;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.METADATA_FIELD;

/**
 * Builder for metadata query qualifier (transferring metadata field)
 **/
@Beta
public class MetadataQualifierBuilder extends BaseQualifierBuilder<MetadataQualifierBuilder> {

    MetadataQualifierBuilder() {
    }

    public CriteriaDefinition.AerospikeMetadata getMetadataField() {
        return (CriteriaDefinition.AerospikeMetadata) map.get(METADATA_FIELD);
    }

    /**
     * Set metadata field. Mandatory parameter for metadata query.
     */
    public MetadataQualifierBuilder setMetadataField(CriteriaDefinition.AerospikeMetadata metadataField) {
        this.map.put(METADATA_FIELD, metadataField);
        return this;
    }

    @Override
    protected void validate() {
        // metadata query validation
        if (this.getMetadataField() != null) {
            if (this.getPath() == null) {
                if (this.getValue() != null) {
                    validateValues();
                } else {
                    throw new IllegalArgumentException("Expecting value parameter to be provided");
                }
            } else {
                throw new IllegalArgumentException("Unexpected parameter for metadata query: path");
            }
        } else {
            throw new IllegalArgumentException("Expecting metadataField parameter to be provided");
        }
    }

    private void validateValues() {
        FilterOperation operation = this.getFilterOperation();
        switch (operation) {
            case EQ, NOTEQ, LT, LTEQ, GT, GTEQ -> Assert.isTrue(getValue().getObject() instanceof Long,
                operation.name() + ": value is expected to be set as Long");
            case BETWEEN -> {
                Assert.isTrue(getSecondValue() != null, "BETWEEN: expecting secondValue to be provided");
                Assert.isTrue(getValue().getObject() instanceof Long,
                    "BETWEEN: value is expected to be set as Long");
                Assert.isTrue(getSecondValue().getObject() instanceof Long,
                    "BETWEEN: secondValue is expected to be set as Long");
            }
            case NOT_IN, IN ->
            {
                Object obj = getValue().getObject();
                //noinspection unchecked
                Assert.isTrue(obj instanceof Collection
                        && !((Collection<Object>) obj).isEmpty()
                        && ((Collection<Object>) obj).toArray()[0] instanceof Long,
                    operation.name() + ": value1 is expected to be a non-empty Collection<Long>");
            }
            default -> throw new IllegalArgumentException("Operation " + operation + " cannot be applied to " +
                "metadataField");
        }
    }
}
