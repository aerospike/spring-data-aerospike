package org.springframework.data.aerospike.query.qualifier;

import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.util.Assert;

import java.util.Collection;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.KEY;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.METADATA_FIELD;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE;

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

    public Object getKeyAsObj() {
        return this.map.get(KEY);
    }

    public MetadataQualifierBuilder setKeyAsObj(Object object) {
        this.map.put(KEY, object);
        return this;
    }

    public Object getValueAsObj() {
        return this.map.get(VALUE);
    }

    /**
     * Set value as Object. Mandatory parameter for metadata query.
     */
    public MetadataQualifierBuilder setValueAsObj(Object object) {
        this.map.put(VALUE, object);
        return this;
    }

    public Object getSecondValueAsObj() {
        return this.map.get(SECOND_VALUE);
    }

    public MetadataQualifierBuilder setSecondValueAsObj(Object object) {
        this.map.put(SECOND_VALUE, object);
        return this;
    }

    @Override
    protected void validate() {
        // metadata query validation
        if (this.getMetadataField() != null) {
            if (this.getPath() == null) {
                if (this.getValueAsObj() != null) {
                    validateValueAsObj();
                } else {
                    throw new IllegalArgumentException("Expecting valueAsObj parameter to be provided");
                }
            } else {
                throw new IllegalArgumentException("Unexpected parameter: bin name (unnecessary for metadata query)");
            }
        } else {
            throw new IllegalArgumentException("Expecting metadataField parameter to be provided");
        }
    }

    private void validateValueAsObj() {
        FilterOperation operation = this.getFilterOperation();
        switch (operation) {
            case EQ, NOTEQ, LT, LTEQ, GT, GTEQ -> Assert.isTrue(getValueAsObj() instanceof Long,
                operation.name() + ": value1 is expected to be set as Long");
            case BETWEEN -> {
                Assert.isTrue(getValueAsObj() instanceof Long,
                    "BETWEEN: value1 is expected to be set as Long");
                Assert.isTrue(getSecondValueAsObj() instanceof Long,
                    "BETWEEN: value2 is expected to be set as Long");
            }
            case NOT_IN, IN -> //noinspection unchecked
                Assert.isTrue(getValueAsObj() instanceof Collection
                        && !((Collection<Object>) getValueAsObj()).isEmpty()
                        && ((Collection<Object>) getValueAsObj()).toArray()[0] instanceof Long,
                    operation.name() + ": value1 is expected to be a non-empty Collection<Long>");
            default -> throw new IllegalArgumentException("Operation " + operation + " cannot be applied to " +
                "metadataField");
        }
    }
}
