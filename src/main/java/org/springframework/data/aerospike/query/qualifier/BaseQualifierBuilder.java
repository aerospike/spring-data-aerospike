package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.query.FilterOperation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.FILTER_OPERATION;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.IGNORE_CASE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.PATH;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE;

@SuppressWarnings("unchecked")
public abstract class BaseQualifierBuilder<T extends BaseQualifierBuilder<?>> implements IQualifierBuilder {

    protected final Map<QualifierKey, Object> map = new HashMap<>();

    public boolean getIgnoreCase() {
        return Boolean.parseBoolean(map.get(IGNORE_CASE).toString());
    }

    public FilterOperation getFilterOperation() {
        return (FilterOperation) map.get(FILTER_OPERATION);
    }

    /**
     * Set FilterOperation for qualifier. Mandatory parameter.
     */
    public T setFilterOperation(FilterOperation operationType) {
        map.put(FILTER_OPERATION, operationType);
        return (T) this;
    }

    public String getPath() {
        return (String) map.get(PATH);
    }

    public Value getValue() {
        return (Value) map.get(VALUE);
    }

    public Value getSecondValue() {
        return (Value) map.get(SECOND_VALUE);
    }

    public Qualifier build() {
        validate();
        return new Qualifier(process(this));
    }

    public Map<QualifierKey, Object> getMap() {
        return Collections.unmodifiableMap(map);
    }

    protected void validate() {
        // do nothing
    }

    protected IQualifierBuilder process(BaseQualifierBuilder<T> builder) {
        // do nothing
        return this;
    }
}
