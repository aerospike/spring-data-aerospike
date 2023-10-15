package org.springframework.data.aerospike.query;

import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.util.Assert;

import java.util.Collection;
import java.util.Map;

import static org.springframework.data.aerospike.query.Qualifier.METADATA_FIELD;
import static org.springframework.data.aerospike.query.Qualifier.VALUE1;
import static org.springframework.data.aerospike.query.Qualifier.VALUE2;

public class MetadataQualifierBuilder extends QualifierBuilder {

    public MetadataQualifierBuilder setMetadataField(CriteriaDefinition.AerospikeMetadata metadataField) {
        this.map.put(METADATA_FIELD, metadataField);
        return this;
    }

    public CriteriaDefinition.AerospikeMetadata getMetadataField() {
        return (CriteriaDefinition.AerospikeMetadata) map.get(METADATA_FIELD);
    }

    public MetadataQualifierBuilder setValue1AsObj(Object object) {
        this.map.put(VALUE1, object);
        return this;
    }

    public Object getValue1AsObj() {
        return this.map.get(VALUE1);
    }

    public MetadataQualifierBuilder setValue2AsObj(Object object) {
        this.map.put(VALUE2, object);
        return this;
    }

    public Object getValue2AsObj() {
        return this.map.get(VALUE2);
    }

    public Map<String, Object> buildMap() {
        return this.map;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void validate() {
        // metadata query
        if (this.getMetadataField() != null) {
            if (this.getField() == null) {
                FilterOperation operation = this.getFilterOperation();
                switch (operation) {
                    case EQ, NOTEQ, LT, LTEQ, GT, GTEQ -> Assert.isTrue(this.getValue1AsObj() instanceof Long,
                        operation.name() + ": value1 is expected to be set as Long");
                    case BETWEEN -> {
                        Assert.isTrue(this.getValue1AsObj() instanceof Long,
                            "BETWEEN: value1 is expected to be set as Long");
                        Assert.isTrue(this.getValue2AsObj() instanceof Long,
                            "BETWEEN: value2 is expected to be set as Long");
                    }
                    case NOT_IN, IN -> Assert.isTrue(this.getValue1AsObj() instanceof Collection
                            && (!((Collection<Object>) this.getValue1AsObj()).isEmpty())
                            && ((Collection<Object>) this.getValue1AsObj()).toArray()[0] instanceof Long,
                        operation.name() + ": value1 is expected to be a non-empty Collection<Long>");
                    default -> throw new IllegalArgumentException("Operation " + operation + " cannot be applied to " +
                        "metadataField");
                }
            } else {
                throw new IllegalArgumentException("Either a field or a metadataField must be set, not both");
            }
        }
    }

}
