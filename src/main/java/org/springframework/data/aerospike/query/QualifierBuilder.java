package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;

import java.util.HashMap;
import java.util.Map;

import static org.springframework.data.aerospike.query.Qualifier.*;

public class QualifierBuilder implements QualifierMapBuilder {

    private final Map<String, Object> map = new HashMap<>();

    public QualifierBuilder setField(String field) {
        this.map.put(FIELD, field);
        return this;
    }

    public String getField() {
        return (String) this.map.get(FIELD);
    }

    public QualifierBuilder setIgnoreCase(boolean ignoreCase) {
        this.map.put(IGNORE_CASE, ignoreCase);
        return this;
    }

    public QualifierBuilder setFilterOperation(FilterOperation filterOperation) {
        this.map.put(OPERATION, filterOperation);
        return this;
    }

    public FilterOperation getFilterOperation() {
        return (FilterOperation) this.map.get(OPERATION);
    }

    public QualifierBuilder setQualifiers(Qualifier... qualifiers) {
        this.map.put(QUALIFIERS, qualifiers);
        return this;
    }

    public QualifierBuilder setExcludeFilter(boolean excludeFilter) {
        this.map.put(EXCLUDE_FILTER, excludeFilter);
        return this;
    }

    public QualifierBuilder setValue1(Value value1) {
        this.map.put(VALUE1, value1);
        return this;
    }

    public QualifierBuilder setValue2(Value value2) {
        this.map.put(VALUE2, value2);
        return this;
    }

    @SuppressWarnings("UnusedReturnValue")
    public QualifierBuilder setValue3(Value value3) {
        this.map.put(VALUE3, value3);
        return this;
    }

    public void setDotPath(String dotPath) {
        this.map.put(DOT_PATH, dotPath);
    }

    public QualifierBuilder setConverter(MappingAerospikeConverter converter) {
        this.map.put(CONVERTER, converter);
        return this;
    }

    public boolean hasValue1() {
        return this.map.get(VALUE1) != null;
    }

    public boolean hasValue2() {
        return this.map.get(VALUE2) != null;
    }

    public boolean hasValue3() {
        return this.map.get(VALUE3) != null;
    }

    public boolean hasDotPath() {
        return this.map.get(DOT_PATH) != null;
    }

    public Qualifier build() {
        return new Qualifier(this);
    }

    public Map<String, Object> buildMap() {
        return this.map;
    }
}
