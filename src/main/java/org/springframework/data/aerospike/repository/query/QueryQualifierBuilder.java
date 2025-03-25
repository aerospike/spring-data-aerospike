package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.BaseQualifierBuilder;
import org.springframework.data.aerospike.query.qualifier.QualifierBuilder;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;

import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.*;

public class QueryQualifierBuilder extends BaseQualifierBuilder<QualifierBuilder> {

    QueryQualifierBuilder() {
    }

    /**
     * Set FilterOperation for qualifier. Mandatory parameter.
     */
    public QueryQualifierBuilder setInnerQbFilterOperation(FilterOperation operationType) {
        map.put(FILTER_OPERATION, operationType);
        return this;
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

    /**
     * Set Map key placeholder (for "Map keys containing" queries).
     */
    public QueryQualifierBuilder setMapKeyPlaceholder() {
        this.map.put(MAP_KEY_PLACEHOLDER, true);
        return this;
    }

    public boolean hasDotPath() {
        return map.get(DOT_PATH) != null;
    }

    /**
     * Set a flag showing that the query has an id expression
     */
    public QueryQualifierBuilder setIsIdExpr(boolean value) {
        this.map.put(IS_ID_EXPR, value);
        return this;
    }
}
