package org.springframework.data.aerospike.query.qualifier;

import org.springframework.data.aerospike.query.FilterOperation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.data.aerospike.query.qualifier.QualifierField.DOT_PATH;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.FIELD;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.KEY;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.OPERATION;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.VALUE;

@SuppressWarnings("unchecked")
abstract class BaseQualifierBuilder<T extends BaseQualifierBuilder<?>> implements IQualifierBuilder {

    protected final Map<QualifierField, Object> map = new HashMap<>();

    public FilterOperation getFilterOperation() {
        return (FilterOperation) this.map.get(OPERATION);
    }

    public T setFilterOperation(FilterOperation filterOperation) {
        this.map.put(OPERATION, filterOperation);
        return (T) this;
    }

    public String getField() {
        return (String) this.map.get(FIELD);
    }

    public boolean hasKey() {
        return this.map.get(KEY) != null;
    }

    public boolean hasValue() {
        return this.map.get(VALUE) != null;
    }

    public boolean hasSecondValue() {
        return this.map.get(SECOND_VALUE) != null;
    }

    public boolean hasDotPath() {
        return this.map.get(DOT_PATH) != null;
    }

    public Qualifier build() {
        validate();
        return new Qualifier(this);
    }

    public Map<QualifierField, Object> getMap() {
        return Collections.unmodifiableMap(this.map);
    }

    protected void validate() {
        // do nothing
    }
}
