package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.dsl.Index;
import org.springframework.data.aerospike.annotation.Beta;

import java.util.Collection;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DSL_INDEXES;
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

    /**
     * Set indexes for combined queries to choose from based on cardinality. Optional parameter.
     */
    public DSLStringQualifierBuilder setIndexes(Collection<Index> indexes) {
        this.map.put(DSL_INDEXES, indexes);
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
