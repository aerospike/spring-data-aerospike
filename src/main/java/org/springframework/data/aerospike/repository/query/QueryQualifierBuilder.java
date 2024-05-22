package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.BaseQualifierBuilder;
import org.springframework.data.aerospike.query.qualifier.QualifierBuilder;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.util.StringUtils;

import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.*;

public class QueryQualifierBuilder extends BaseQualifierBuilder<QualifierBuilder> {

    QueryQualifierBuilder() {
    }

    public QueryQualifierBuilder setIgnoreCase(boolean ignoreCase) {
        this.map.put(IGNORE_CASE, ignoreCase);
        return this;
    }

    /**
     * Set bin name. Mandatory parameter for bin query.
     */
    public QueryQualifierBuilder setBinName(String field) {
        this.map.put(BIN_NAME, field);
        return this;
    }

    /**
     * Set bin type.
     */
    public QueryQualifierBuilder setBinType(Exp.Type type) {
        this.map.put(BIN_TYPE, type);
        return this;
    }

    /**
     * Set full path from bin name to required element.
     */
    public QueryQualifierBuilder setDotPath(List<String> dotPath) {
        this.map.put(DOT_PATH, dotPath);
        return this;
    }

    /**
     * Set context path.
     */
    public QueryQualifierBuilder setCtx(String ctx) {
        this.map.put(CTX_PATH, ctx);
        return this;
    }

    /**
     * Set context path.
     */
    public QueryQualifierBuilder setCtxList(List<String> ctxList) {
        this.map.put(CTX_LIST, ctxList);
        return this;
    }

    /**
     * Set CTX[].
     */
    public QueryQualifierBuilder setCtxArray(CTX[] ctxArray) {
        this.map.put(CTX_ARRAY, ctxArray);
        return this;
    }

    /**
     * Set Map key.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * key into a {@link Value} object.
     */
    public QueryQualifierBuilder setKey(Value key) {
        this.map.put(KEY, key);
        return this;
    }

    /**
     * For "find by one level nested map containing" queries. Set nested Map key.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * key into a {@link Value} object.
     */
    public QueryQualifierBuilder setNestedKey(Value key) {
        this.map.put(NESTED_KEY, key);
        return this;
    }

    /**
     * Set value. Mandatory parameter for bin query for all operations except {@link FilterOperation#IS_NOT_NULL} and
     * {@link FilterOperation#IS_NULL}.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * value into a {@link Value} object.
     */
    public QueryQualifierBuilder setValue(Value value) {
        this.map.put(VALUE, value);
        return this;
    }

    /**
     * Set second value.
     * <p>
     * Use one of the Value get() methods ({@link Value#get(int)}, {@link Value#get(String)} etc.) to firstly read the
     * second value into a {@link Value} object.
     */
    public QueryQualifierBuilder setSecondValue(Value secondValue) {
        this.map.put(SECOND_VALUE, secondValue);
        return this;
    }

    /**
     * For "find by one level nested map containing" queries. Set the type of the nested map value using
     * {@link ParticleType}.
     */
    public QueryQualifierBuilder setNestedType(int type) {
        this.map.put(NESTED_TYPE, type);
        return this;
    }

    /**
     * Set server version support.
     */
    public QueryQualifierBuilder setServerVersionSupport(ServerVersionSupport serverVersionSupport) {
        this.map.put(SERVER_VERSION_SUPPORT, serverVersionSupport);
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
