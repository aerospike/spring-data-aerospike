package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;

import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.QualifierField.CONVERTER;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.DOT_PATH;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.FIELD;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.IGNORE_CASE;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.KEY;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierField.VALUE;

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

    public QualifierBuilder setDotPath(List<String> dotPath) {
        this.map.put(DOT_PATH, dotPath);
        return this;
    }

    public QualifierBuilder setConverter(MappingAerospikeConverter converter) {
        this.map.put(CONVERTER, converter);
        return this;
    }
}
