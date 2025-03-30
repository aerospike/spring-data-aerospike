package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.exp.Expression;
import org.springframework.data.aerospike.annotation.Beta;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.FILTER_EXPRESSION;

@Beta
public class FilterQualifierBuilder extends BaseQualifierBuilder<FilterQualifierBuilder> {

    FilterQualifierBuilder() {
    }

    /**
     * Set filter expression. Mandatory parameter.
     */
    public FilterQualifierBuilder setFilterExpression(Expression filterExpression) {
        this.map.put(FILTER_EXPRESSION, filterExpression);
        return this;
    }

    public Expression getFilterExpression() {
        return (Expression) map.get(FILTER_EXPRESSION);
    }

    @Override
    protected void validate() {
        if (this.getFilterExpression() == null) {
            throw new IllegalArgumentException("Expecting filter expression to be provided");
        }
    }
}
