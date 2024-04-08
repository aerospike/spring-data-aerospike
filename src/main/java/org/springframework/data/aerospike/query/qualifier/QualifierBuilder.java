package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;

import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.CONVERTER;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.DOT_PATH;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.FIELD;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.IGNORE_CASE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.KEY;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE_TYPE;

public class QualifierBuilder extends BaseQualifierBuilder<QualifierBuilder> {

    QualifierBuilder() {
    }

    public QualifierBuilder setIgnoreCase(boolean ignoreCase) {
        this.map.put(IGNORE_CASE, ignoreCase);
        return this;
    }

    public QualifierBuilder setField(String field) {
        this.map.put(FIELD, field);
        return this;
    }

    public QualifierBuilder setKey(Value key) {
        this.map.put(KEY, key);
        return this;
    }

    public QualifierBuilder setValue(Value value) {
        this.map.put(VALUE, value);
        return this;
    }

    public QualifierBuilder setSecondValue(Value secondValue) {
        this.map.put(SECOND_VALUE, secondValue);
        return this;
    }

    /**
     * Set value type using {@link ParticleType}.
     */
    public QualifierBuilder setValueType(int type) {
        this.map.put(VALUE_TYPE, type);
        return this;
    }

    public QualifierBuilder setDotPath(List<String> dotPath) {
        this.map.put(DOT_PATH, dotPath);
        return this;
    }

    public QualifierBuilder setConverter(MappingAerospikeConverter converter) {
        this.map.put(CONVERTER, converter);
        return this;
    }
}
