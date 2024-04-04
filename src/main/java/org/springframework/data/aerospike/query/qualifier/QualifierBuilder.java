package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;

import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DOT_PATH;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.FIELD;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.IGNORE_CASE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.KEY;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE;

public class QualifierBuilder extends BaseQualifierBuilder<QualifierBuilder> {

    QualifierBuilder() {
    }

    public QualifierBuilder setIgnoreCase(boolean ignoreCase) {
        this.map.put(IGNORE_CASE, ignoreCase);
        return this;
    }

    /**
     * Set bin name.
     */
    public QualifierBuilder setField(String field) {
        this.map.put(FIELD, field);
        return this;
    }

    /**
     * Set Map key.
     * <p>
     * Use one of Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * key into a {@link Value} object.
     */
    public QualifierBuilder setKey(Value key) {
        this.map.put(KEY, key);
        return this;
    }

    /**
     * Set value.
     * <p>
     * Use one of Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * value into a {@link Value} object.
     */
    public QualifierBuilder setValue(Value value) {
        this.map.put(VALUE, value);
        return this;
    }

    /**
     * Set second value.
     * <p>
     * Use one of Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * second value into a {@link Value} object.
     */
    public QualifierBuilder setSecondValue(Value secondValue) {
        this.map.put(SECOND_VALUE, secondValue);
        return this;
    }

    /**
     * Required only for a nested value query (e.g. find by a field of a POJO).
     */
    public QualifierBuilder setDotPath(List<String> dotPath) {
        this.map.put(DOT_PATH, dotPath);
        return this;
    }
}
