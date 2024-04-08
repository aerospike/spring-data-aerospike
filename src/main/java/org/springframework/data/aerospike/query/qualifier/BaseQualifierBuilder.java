package org.springframework.data.aerospike.query.qualifier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.data.aerospike.config.AerospikeDataConfigurationSupport;
import org.springframework.data.aerospike.config.AerospikeSettings;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.query.FilterOperation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DOT_PATH;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.FIELD;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.KEY;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.OPERATION;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE;

@SuppressWarnings("unchecked")
abstract class BaseQualifierBuilder<T extends BaseQualifierBuilder<?>> implements IQualifierBuilder {

    protected final Map<QualifierKey, Object> map = new HashMap<>();

    public FilterOperation getFilterOperation() {
        return (FilterOperation) map.get(OPERATION);
    }

    public T setFilterOperation(FilterOperation filterOperation) {
        map.put(OPERATION, filterOperation);
        return (T) this;
    }

    public String getField() {
        return (String) map.get(FIELD);
    }

    public boolean hasKey() {
        return map.get(KEY) != null;
    }

    public boolean hasValue() {
        return map.get(VALUE) != null;
    }

    public boolean hasSecondValue() {
        return map.get(SECOND_VALUE) != null;
    }

    public boolean hasDotPath() {
        return map.get(DOT_PATH) != null;
    }

    public Qualifier build() {
        validate();
        return new Qualifier(this);
    }

    public Map<QualifierKey, Object> getMap() {
        return Collections.unmodifiableMap(map);
    }

    protected void validate() {
        // do nothing
    }
}
