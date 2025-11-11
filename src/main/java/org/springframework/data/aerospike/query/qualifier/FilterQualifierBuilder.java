package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.Filter;
import org.springframework.data.aerospike.annotation.Beta;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.FILTER_EXPRESSION;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SINDEX_FILTER;

/**
 * Builder for filter qualifier (transferring secondary index {@link Filter} and filtering {@link Expression})
 **/
@Beta
public class FilterQualifierBuilder extends BaseQualifierBuilder<FilterQualifierBuilder> {

    FilterQualifierBuilder() {
    }

    /**
     * Set filter expression. Mandatory parameter.
     */
    public FilterQualifierBuilder setExpression(Expression filterExpression) {
        this.map.put(FILTER_EXPRESSION, filterExpression);
        return this;
    }

    public Expression getExpression() {
        return (Expression) map.get(FILTER_EXPRESSION);
    }

    /**
     * Set secondary index filter. Mandatory parameter.
     */
    public FilterQualifierBuilder setFilter(Filter filter) {
        this.map.put(SINDEX_FILTER, filter);
        return this;
    }

    public Filter getFilter() {
        return (Filter) map.get(SINDEX_FILTER);
    }
}
