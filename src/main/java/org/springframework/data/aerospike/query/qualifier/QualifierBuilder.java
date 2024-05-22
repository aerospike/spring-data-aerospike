package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.util.StringUtils;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.IGNORE_CASE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.PATH;
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
     * Set full path. Mandatory parameter for bin query.
     */
    public QualifierBuilder setPath(String field) {
        this.map.put(PATH, field);
        return this;
    }

    /**
     * Set value. Mandatory parameter for bin query for all operations except {@link FilterOperation#IS_NOT_NULL} and
     * {@link FilterOperation#IS_NULL}.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * value into a {@link Value} object.
     */
    public QualifierBuilder setValue(Value value) {
        this.map.put(VALUE, value);
        return this;
    }

    /**
     * Set second value.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * second value into a {@link Value} object.
     */
    public QualifierBuilder setSecondValue(Value secondValue) {
        this.map.put(SECOND_VALUE, secondValue);
        return this;
    }

    protected void validate() {
        if (!StringUtils.hasText(this.getPath())) {
            throw new IllegalArgumentException("Expecting bin name parameter to be provided");
        }

        if (this.getFilterOperation() == null) {
            throw new IllegalArgumentException("Expecting filter operation parameter to be provided");
        }

        if (this.getValue() == null
            && this.getFilterOperation() != FilterOperation.IS_NULL
            && this.getFilterOperation() != FilterOperation.IS_NOT_NULL) {
            throw new IllegalArgumentException("Expecting value parameter to be provided");
        }
    }
}
