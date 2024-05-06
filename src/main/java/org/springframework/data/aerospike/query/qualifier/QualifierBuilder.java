package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;

import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.*;

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
    public QualifierBuilder setBinName(String field) {
        this.map.put(BIN_NAME, field);
        return this;
    }

    /**
     * Set bin name.
     */
    public QualifierBuilder setBinType(Exp.Type type) {
        this.map.put(BIN_TYPE, type);
        return this;
    }

    /**
     * Set full path from bin name to required element
     */
    public QualifierBuilder setDotPath(List<String> dotPath) {
        this.map.put(DOT_PATH, dotPath);
        return this;
    }

    /**
     * Set context path.
     */
    public QualifierBuilder setCtx(String ctx) {
        this.map.put(CTX_PATH, ctx);
        return this;
    }

    /**
     * Set context path.
     */
    public QualifierBuilder setCtxList(List<String> ctxList) {
        this.map.put(CTX_LIST, ctxList);
        return this;
    }

    /**
     * Set Map key.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * key into a {@link Value} object.
     */
    public QualifierBuilder setKey(Value key) {
        this.map.put(KEY, key);
        return this;
    }

    /**
     * For "find by one level nested map containing" queries. Set nested Map key.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * key into a {@link Value} object.
     */
    public QualifierBuilder setNestedKey(Value key) {
        this.map.put(NESTED_KEY, key);
        return this;
    }

    /**
     * Set value.
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

    /**
     * For "find by one level nested map containing" queries. Set the type of the nested map value using
     * {@link ParticleType}.
     */
    public QualifierBuilder setNestedType(int type) {
        this.map.put(NESTED_TYPE, type);
        return this;
    }
}
