package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.annotation.Beta;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.FilterOperation;

import java.util.List;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.FILTER_OPERATION;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SECOND_VALUE;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.SINDEX_NAME;
import static org.springframework.data.aerospike.query.qualifier.QualifierKey.VALUE;


/**
 * Qualifier builder for a query indexed using an Expression
 * (see {@link AerospikeTemplate#createIndex(String, String, IndexType, IndexCollectionType, Expression)})
 */
@Beta
public class IndexedWithExpressionQualifierBuilder extends BaseQualifierBuilder<IndexedWithExpressionQualifierBuilder> {

    IndexedWithExpressionQualifierBuilder() {
    }

    /**
     * Set the name of the existing secondary index to use. Mandatory parameter.
     */
    public IndexedWithExpressionQualifierBuilder setIndexName(String dslString) {
        map.put(SINDEX_NAME, dslString);
        return this;
    }

    private boolean hasSecondaryIndexName() {
        return map.get(SINDEX_NAME) != null;
    }

    /**
     * Set FilterOperation. Mandatory parameter.
     */
    public IndexedWithExpressionQualifierBuilder setFilterOperation(FilterOperation operationType) {
        map.put(FILTER_OPERATION, operationType);
        return this;
    }

    /**
     * Set value. Mandatory parameter for most of filter operations.
     *
     * @param value The provided object will be read into a {@link Value},
     *              so its type must be recognizable by {@link Value#get(Object)}.
     */
    public IndexedWithExpressionQualifierBuilder setValue(Object value) {
        this.map.put(VALUE, Value.get(value));
        return this;
    }

    /**
     * Set second value. Optional parameter, required for filter operations like BETWEEN.
     *
     * @param secondValue The provided object will be read into a {@link Value},
     *              so its type must be recognizable by {@link Value#get(Object)}.
     * */
    public IndexedWithExpressionQualifierBuilder setSecondValue(Object secondValue) {
        this.map.put(SECOND_VALUE, Value.get(secondValue));
        return this;
    }

    @Override
    protected void validate() {
        if (!hasSecondaryIndexName()) {
            throw new IllegalStateException("Expecting existing secondary index name to be provided");
        }
        if (getFilterOperation() == null) {
            throw new IllegalStateException("Expecting filter operation to be provided");
        }

        if (this.getValue() == null
            && this.getFilterOperation() != FilterOperation.IS_NULL
            && this.getFilterOperation() != FilterOperation.IS_NOT_NULL) {
            throw new IllegalArgumentException("Expecting value be provided");
        }

        List<FilterOperation> betweenList = List.of(FilterOperation.BETWEEN, FilterOperation.MAP_VAL_BETWEEN_BY_KEY,
            FilterOperation.MAP_VAL_BETWEEN, FilterOperation.MAP_KEYS_BETWEEN, FilterOperation.COLLECTION_VAL_BETWEEN);
        if (betweenList.contains(this.getFilterOperation())
            && ((this.getValue() == null || this.getSecondValue() == null)
            || (this.getValue().getObject() == null || this.getSecondValue().getObject() == null))) {
            throw new IllegalArgumentException(this.getFilterOperation() + ": expecting both value and secondValue " +
                "to be provided");
        }
    }
}
