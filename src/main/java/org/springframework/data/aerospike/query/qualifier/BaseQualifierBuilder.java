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
        return  (Boolean) map.getOrDefault(IGNORE_CASE, false);
    }

    public FilterOperation getFilterOperation() {
        return (FilterOperation) map.get(FILTER_OPERATION);
    }

    /**
     * Set FilterOperation. Mandatory parameter.
     */
    public T setFilterOperation(FilterOperation operationType) {
        map.put(FILTER_OPERATION, operationType);
        return (T) this;
    }

    /**
     * Set value. Mandatory parameter for bin or metadata query for all operations
     * except {@link FilterOperation#IS_NOT_NULL} and {@link FilterOperation#IS_NULL}.
     * <p>
     *
     * @param value The provided object will be read into a {@link Value},
     *              so its type must be recognizable by {@link Value#get(Object)}.
     */
    public T setValue(Object value) {
        this.map.put(VALUE, Value.get(value));
        return (T) this;
    }

    /**
     * Set second value.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * second value into a {@link Value} object.
     *
     * @param secondValue The provided object will be read into a {@link Value},
     *              so its type must be recognizable by {@link Value#get(Object)}.
     * */
    public T setSecondValue(Object secondValue) {
        this.map.put(SECOND_VALUE, Value.get(secondValue));
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
