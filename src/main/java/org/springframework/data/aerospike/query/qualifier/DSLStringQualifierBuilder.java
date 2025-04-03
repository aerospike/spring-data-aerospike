package org.springframework.data.aerospike.query.qualifier;

import org.springframework.data.aerospike.annotation.Beta;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DSL_STRING;

@Beta
public class DSLStringQualifierBuilder extends BaseQualifierBuilder<DSLStringQualifierBuilder> {

    DSLStringQualifierBuilder() {
    }

    /**
     * Set DSL String. Mandatory parameter.
     */
    public DSLStringQualifierBuilder setDSLString(String dslString) {
        this.map.put(DSL_STRING, dslString);
        return this;
    }

    public String getDSLString() {
        return (String) map.get(DSL_STRING);
    }

    @Override
    protected void validate() {
        if (this.getDSLString() == null) {
            throw new IllegalArgumentException("Expecting DSL String to be provided");
        }
    }
}
