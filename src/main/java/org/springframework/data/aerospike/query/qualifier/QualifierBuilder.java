package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import org.springframework.data.aerospike.annotation.Beta;
import org.springframework.data.aerospike.index.AerospikeIndexResolverUtils;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.repository.query.QueryQualifierBuilder;
import org.springframework.util.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.function.Predicate.not;
import static org.springframework.data.aerospike.index.AerospikeIndexResolverUtils.getCtxType;
import static org.springframework.data.aerospike.index.AerospikeIndexResolverUtils.isCtxMapKey;
import static org.springframework.data.aerospike.index.AerospikeIndexResolverUtils.isCtxMapValue;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.IGNORE_CASE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.PATH;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE;

@Beta
public class QualifierBuilder extends BaseQualifierBuilder<QualifierBuilder> {

    QualifierBuilder() {
    }

    public QualifierBuilder setIgnoreCase(boolean ignoreCase) {
        this.map.put(IGNORE_CASE, ignoreCase);
        return this;
    }

    /**
     * Dot separated path consisting of bin name and optional context. Mandatory parameter for bin query. Current
     * limitation: if context is given, its last element must be either a List, a Map or a Map key, not Map value.
     * <br><br>
     * Context is provided using the following DSL.
     * <br>
     * <table border="1">
     *   <tr>
     *     <td> a </td> <td> Map key “a” </td>
     *   </tr>
     *   <tr>
     *     <td> '1' </td> <td> Map key (String) “1” </td>
     *   </tr>
     *   <tr>
     *     <td> 1 </td> <td> Map key 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {1} </td> <td> Map index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=1} </td> <td> Map value (int) 1 </td>
     *   </tr>
     *   <tr>
     *     <td> {=bb} </td> <td> Map value “bb” </td>
     *   </tr>
     *   <tr>
     *     <td> {='1'} </td> <td> Map value (String) “1” </td>
     *   </tr>
     *     <td> {#1} </td> <td> Map rank 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [1] </td> <td> List index 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [=1] </td> <td> List value 1 </td>
     *   </tr>
     *   <tr>
     *     <td> [#1] </td> <td> List rank 1 </td>
     * </table>
     * <br>
     * Examples:
     * <table border="1">
     *   <tr>
     *     <td> binName </td> <td> [binName] </td>
     *   </tr>
     *   <tr>
     *     <td> mapBinName.k </td> <td> [mapBinName -> mapKey("a")] </td>
     *   </tr>
     *   <tr>
     *     <td> mapBinName.a.aa.aaa </td> <td> [mapBinName -> mapKey("a") -> mapKey("aa") -> mapKey("aaa")] </td>
     *   </tr>
     *   <tr>
     *     <td> mapBinName.a.55 </td> <td> [mapBinName -> mapKey("a") -> mapKey(55)] </td>
     *   </tr>
     *   <tr>
     *     <td> listBinName.[1].aa </td> <td> [listBinName -> listIndex(1) -> mapKey("aa")] </td>
     *   </tr>
     *   <tr>
     *     <td> mapBinName.ab.cd.[-1].'10' </td> <td> [mapBinName -> mapKey("ab") -> mapKey("cd") -> listIndex(-1) ->
     *         mapKey("10")] </td>
     *   </tr>
     * </table>
     */
    public QualifierBuilder setPath(String path) {
        this.map.put(PATH, path);
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
            throw new IllegalArgumentException("Expecting path parameter to be provided");
        }

        if (this.getFilterOperation() == null) {
            throw new IllegalArgumentException("Expecting operation type parameter to be provided");
        }

        if (this.getValue() == null
            && this.getFilterOperation() != FilterOperation.IS_NULL
            && this.getFilterOperation() != FilterOperation.IS_NOT_NULL) {
            throw new IllegalArgumentException("Expecting value parameter to be provided");
        }

        List<FilterOperation> betweenList = List.of(FilterOperation.BETWEEN, FilterOperation.MAP_VAL_BETWEEN_BY_KEY,
            FilterOperation.MAP_VAL_BETWEEN, FilterOperation.MAP_KEYS_BETWEEN, FilterOperation.COLLECTION_VAL_BETWEEN);
        if (betweenList.contains(this.getFilterOperation()) &&
            (this.getValue() == null || this.getSecondValue() == null)) {
            throw new IllegalArgumentException(this.getFilterOperation() + ": expecting both value and secondValue " +
                "to be provided");
        }
    }

    protected IQualifierBuilder process(BaseQualifierBuilder<QualifierBuilder> externalQb) {
        return getInnerQb(externalQb);
    }

    private QueryQualifierBuilder getInnerQb(BaseQualifierBuilder<QualifierBuilder> externalBuilder) {
        QueryQualifierBuilder innerQb;
        try {
            innerQb = getInnerQualifierBuilderInstance();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot initialize QueryQualifierBuilder", e.getCause());
        }
        setInnerQbOrFail(innerQb, externalBuilder.getPath());
        innerQb.setInnerQbFilterOperation(externalBuilder.getFilterOperation());
        innerQb.setValue(externalBuilder.getValue());
        if (externalBuilder.getSecondValue() != null) innerQb.setSecondValue(externalBuilder.getSecondValue());
        if (externalBuilder.getIgnoreCase()) innerQb.setIgnoreCase(true);
        return innerQb;
    }

    protected QueryQualifierBuilder getInnerQualifierBuilderInstance() throws ClassNotFoundException,
        NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<?> qbClass = Class.forName("org.springframework.data.aerospike.repository.query.QueryQualifierBuilder");
        Constructor<?> ctor = qbClass.getDeclaredConstructor();
        ctor.setAccessible(true);
        return (QueryQualifierBuilder) ctor.newInstance();
    }

    private void setInnerQbOrFail(QueryQualifierBuilder innerQb, String path) {
        CTX[] ctxArr = resolveCtxPath(path);

        if (ctxArr.length >= 1) {
            if (!isCtxMapKey(ctxArr[0])) {
                throw new IllegalArgumentException(String.format("Cannot resolve the given path '%s', expecting the " +
                    "first element as a " +
                    "simple String", path));
            }
            innerQb.setBinName(ctxArr[0].value.toString());
            if (ctxArr.length == 1) return; // length == 1

            CTX lastElement = ctxArr[ctxArr.length - 1];
            if (isCtxMapValue(lastElement)) {
                throw new UnsupportedOperationException(String.format("Unsupported path '%s', expecting the last " +
                    "element not to be a Map value", path));
            }
            innerQb.setKey(lastElement.value); // the last element, if present, is map key

            if (ctxArr.length > 2) {
                // CTX path: elements between the first and the last
                innerQb.setCtxArray(Arrays.copyOfRange(ctxArr, 1, ctxArr.length - 1));
                CTX secondElement = ctxArr[1];
                innerQb.setBinType(getCtxType(secondElement)); // finding bin type by looking at the first CTX element
            } else { // length == 2
                innerQb.setBinType(getCtxType(lastElement)); // finding bin type
            }
        } else {
            throw new IllegalArgumentException(String.format("Cannot resolve the given path '%s'", path));
        }
    }

    private CTX[] resolveCtxPath(String path) {
        if (path == null) return null;

        return Arrays.stream(path.split("\\."))
            .filter(not(String::isEmpty))
            .map(AerospikeIndexResolverUtils::toCtx)
            .filter(Objects::nonNull)
            .toArray(CTX[]::new);
    }
}
