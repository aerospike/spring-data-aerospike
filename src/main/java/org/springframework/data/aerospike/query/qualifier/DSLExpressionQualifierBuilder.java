package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.dsl.api.DSLParser;
import org.springframework.data.aerospike.annotation.Beta;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DSL_EXPR_INDEX_TO_USE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DSL_EXPR_STRING;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DSL_EXPR_VALUES;


/**
 * Builder for DSL expression query qualifier (transferring DSL string and placeholder values for {@link DSLParser})
 */
@Beta
public class DSLExpressionQualifierBuilder extends BaseQualifierBuilder<DSLExpressionQualifierBuilder> {

    DSLExpressionQualifierBuilder() {
    }

    /**
     * Set DSL expression String. Mandatory parameter.
     */
    public DSLExpressionQualifierBuilder setDSLExpressionString(String dslString) {
        map.put(DSL_EXPR_STRING, dslString);
        return this;
    }

    /**
     * Set the name of secondary index to use for DSL expression String. Optional parameter.
     */
    public DSLExpressionQualifierBuilder setDSLExpressionIndexToUse(String dslString) {
        map.put(DSL_EXPR_INDEX_TO_USE, dslString);
        return this;
    }

    /**
     * Set DSL expression placeholders values. Optional parameter.
     */
    public DSLExpressionQualifierBuilder setDSLExpressionValues(Object[] values) {
        map.put(DSL_EXPR_VALUES, values);
        return this;
    }

    private boolean hasDslExprString() {
        return map.get(DSL_EXPR_STRING) != null;
    }

    @Override
    protected void validate() {
        if (!hasDslExprString()) {
            throw new IllegalStateException("Expecting DSL String to be provided");
        }
    }
}
