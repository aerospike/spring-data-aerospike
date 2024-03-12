/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.springframework.data.aerospike.query;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generic Bin qualifier. It acts as a filter to exclude records that do not meet the given criteria.
 * <p>
 * For the list of the supported operations see {@link FilterOperation}
 *
 * @author Peter Milne
 */
public class Qualifier implements CriteriaDefinition, Map<String, Object>, Serializable {

    protected static final String FIELD = "field";
    protected static final String METADATA_FIELD = "metadata_field";
    protected static final String SINGLE_ID_FIELD = "id";
    protected static final String MULTIPLE_IDS_FIELD = "ids";
    protected static final String IGNORE_CASE = "ignoreCase";
    protected static final String QUERY_PARAMETERS = "queryParameters";

    protected static final String KEY = "key";
    protected static final String VALUE = "value";
    protected static final String SECOND_VALUE = "value2";
    protected static final String DOT_PATH = "dotPath";
    protected static final String CONVERTER = "converter";
    protected static final String QUALIFIERS = "qualifiers";
    protected static final String OPERATION = "operation";
    protected static final String AS_FILTER = "queryAsFilter";
    @Serial
    private static final long serialVersionUID = -2689196529952712849L;
    protected final Map<String, Object> internalMap = new HashMap<>();

    protected Qualifier(Qualifier.Builder builder) {
        if (!builder.getMap().isEmpty()) {
            internalMap.putAll(builder.getMap());
        }
    }

    protected Qualifier(Qualifier qualifier) {
        if (!qualifier.getMap().isEmpty()) {
            internalMap.putAll(qualifier.getMap());
        }
    }

    @Override
    public Qualifier getCriteriaObject() {
        return this;
    }

    @Override
    public String geField() {
        return this.getField();
    }

    private Map<String, Object> getMap() {
        return Collections.unmodifiableMap(this.internalMap);
    }

    public static QualifierBuilder builder() {
        return new QualifierBuilder();
    }

    public static MetadataQualifierBuilder metadataBuilder() {
        return new MetadataQualifierBuilder();
    }

    public FilterOperation getOperation() {
        return (FilterOperation) internalMap.get(OPERATION);
    }

    public String getField() {
        return (String) internalMap.get(FIELD);
    }

    public CriteriaDefinition.AerospikeMetadata getMetadataField() {
        return (CriteriaDefinition.AerospikeMetadata) internalMap.get(METADATA_FIELD);
    }

    public void setQueryAsFilter(Boolean queryAsFilter) {
        internalMap.put(AS_FILTER, queryAsFilter);
    }

    public Boolean queryAsFilter() {
        return internalMap.containsKey(AS_FILTER) && (Boolean) internalMap.get(AS_FILTER);
    }

    public boolean hasQualifiers() {
        return internalMap.get(QUALIFIERS) != null;
    }

    public boolean hasId() {
        return internalMap.get(SINGLE_ID_FIELD) != null || internalMap.get(MULTIPLE_IDS_FIELD) != null;
    }

    public boolean hasSingleId() {
        return internalMap.get(SINGLE_ID_FIELD) != null;
    }

    public Object getId() {
        return this.hasSingleId() ? internalMap.get(SINGLE_ID_FIELD) : internalMap.get(MULTIPLE_IDS_FIELD);
    }

    public Qualifier[] getQualifiers() {
        return (Qualifier[]) internalMap.get(QUALIFIERS);
    }

    public Value getKey() {
        return (Value) internalMap.get(KEY);
    }

    public Value getValue() {
        return (Value) internalMap.get(VALUE);
    }

    public Value getSecondValue() {
        return (Value) internalMap.get(SECOND_VALUE);
    }

    @SuppressWarnings("unchecked")
    public List<Object> getQueryParameters() { return (List<Object>) internalMap.get(QUERY_PARAMETERS); }

    @SuppressWarnings("unchecked")
    public List<String> getDotPath() {
        return (List<String>) internalMap.get(DOT_PATH);
    }

    public Filter setQueryAsFilter() {
        return FilterOperation.valueOf(getOperation().toString()).sIndexFilter(internalMap);
    }

    public Exp toFilterExp() {
        return FilterOperation.valueOf(getOperation().toString()).filterExp(internalMap);
    }

    protected String luaFieldString(String field) {
        return String.format("rec['%s']", field);
    }

    protected String luaValueString(Value value) {
        if (null == value) return null;
        int type = value.getType();
        return switch (type) {
            case ParticleType.STRING, ParticleType.GEOJSON -> String.format("'%s'", value);
            default -> value.toString();
        };
    }

    @Override
    public int size() {
        return internalMap.size();
    }

    @Override
    public boolean isEmpty() {
        return internalMap.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return internalMap.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return internalMap.containsValue(value);
    }

    @Override
    public Object get(Object key) {
        return internalMap.get(key);
    }

    @Override
    public Object put(String key, Object value) {
        return internalMap.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return internalMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        internalMap.putAll(m);
    }

    @Override
    public void clear() {
        internalMap.clear();
    }

    @Override
    public Set<String> keySet() {
        return internalMap.keySet();
    }

    @Override
    public Collection<Object> values() {
        return internalMap.values();
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        return internalMap.entrySet();
    }

    @Override
    public String toString() {
        if (!StringUtils.hasLength(getField()) && StringUtils.hasLength(getMetadataField().toString())) {
            return String.format("%s:%s:%s:%s", getField(), getOperation(), getKey(), getValue());
        }
        return String.format("(metadata) %s:%s:%s:%s", getMetadataField().toString(),
            getOperation(), getKey(), getValue());
    }

    protected interface Builder {

        Map<String, Object> getMap();

        Qualifier build();
    }

    public static class QualifierBuilder extends BaseQualifierBuilder<QualifierBuilder> {

        private QualifierBuilder() {
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

        public boolean hasValue3() {
            return this.map.get(SECOND_VALUE) != null;
        }

        public QualifierBuilder setDotPath(List<String> dotPath) {
            this.map.put(DOT_PATH, dotPath);
            return this;
        }

        public QualifierBuilder setConverter(MappingAerospikeConverter converter) {
            this.map.put(CONVERTER, converter);
            return this;
        }

        public QualifierBuilder setQueryParameters(List<Object> queryParameters) {
            this.map.put(QUERY_PARAMETERS, queryParameters);
            return this;
        }
    }

    public static class MetadataQualifierBuilder extends BaseQualifierBuilder<MetadataQualifierBuilder> {

        private MetadataQualifierBuilder() {
        }

        public CriteriaDefinition.AerospikeMetadata getMetadataField() {
            return (CriteriaDefinition.AerospikeMetadata) map.get(METADATA_FIELD);
        }

        public MetadataQualifierBuilder setMetadataField(CriteriaDefinition.AerospikeMetadata metadataField) {
            this.map.put(METADATA_FIELD, metadataField);
            return this;
        }

        public Object getKeyAsObj() {
            return this.map.get(KEY);
        }

        public MetadataQualifierBuilder setKeyAsObj(Object object) {
            this.map.put(KEY, object);
            return this;
        }

        public Object getValueAsObj() {
            return this.map.get(VALUE);
        }

        public MetadataQualifierBuilder setValueAsObj(Object object) {
            this.map.put(VALUE, object);
            return this;
        }

        public Object getSecondValueAsObj() {
            return this.map.get(SECOND_VALUE);
        }

        public MetadataQualifierBuilder setSecondValueAsObj(Object object) {
            this.map.put(SECOND_VALUE, object);
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void validate() {
            // metadata query
            if (this.getMetadataField() != null) {
                if (this.getField() == null) {
                    FilterOperation operation = this.getFilterOperation();
                    switch (operation) {
                        case EQ, NOTEQ, LT, LTEQ, GT, GTEQ -> Assert.isTrue(getValueAsObj() instanceof Long,
                            operation.name() + ": value1 is expected to be set as Long");
                        case BETWEEN -> {
                            Assert.isTrue(getValueAsObj() instanceof Long,
                                "BETWEEN: value1 is expected to be set as Long");
                            Assert.isTrue(getSecondValueAsObj() instanceof Long,
                                "BETWEEN: value2 is expected to be set as Long");
                        }
                        case NOT_IN, IN -> Assert.isTrue(getValueAsObj() instanceof Collection
                                && (!((Collection<Object>) getValueAsObj()).isEmpty())
                                && ((Collection<Object>) getValueAsObj()).toArray()[0] instanceof Long,
                            operation.name() + ": value1 is expected to be a non-empty Collection<Long>");
                        default ->
                            throw new IllegalArgumentException("Operation " + operation + " cannot be applied to " +
                                "metadataField");
                    }
                } else {
                    throw new IllegalArgumentException("Either a field or a metadataField must be set, not both");
                }
            }
        }
    }

    private static class IdQualifierBuilder extends BaseQualifierBuilder<IdQualifierBuilder> {

        private IdQualifierBuilder() {
        }

        private IdQualifierBuilder setId(String id) {
            this.map.put(SINGLE_ID_FIELD, id);
            return this;
        }

        private IdQualifierBuilder setId(Short id) {
            this.map.put(SINGLE_ID_FIELD, id);
            return this;
        }

        private IdQualifierBuilder setId(Integer id) {
            this.map.put(SINGLE_ID_FIELD, id);
            return this;
        }

        private IdQualifierBuilder setId(Long id) {
            this.map.put(SINGLE_ID_FIELD, id);
            return this;
        }

        private IdQualifierBuilder setId(Character id) {
            this.map.put(SINGLE_ID_FIELD, id);
            return this;
        }

        private IdQualifierBuilder setId(Byte id) {
            this.map.put(SINGLE_ID_FIELD, id);
            return this;
        }

        private IdQualifierBuilder setId(byte[] id) {
            this.map.put(SINGLE_ID_FIELD, id);
            return this;
        }

        private IdQualifierBuilder setIds(String... ids) {
            this.map.put(MULTIPLE_IDS_FIELD, ids);
            return this;
        }

        private IdQualifierBuilder setIds(Short... ids) {
            this.map.put(MULTIPLE_IDS_FIELD, ids);
            return this;
        }

        private IdQualifierBuilder setIds(Integer... ids) {
            this.map.put(MULTIPLE_IDS_FIELD, ids);
            return this;
        }

        private IdQualifierBuilder setIds(Long... ids) {
            this.map.put(MULTIPLE_IDS_FIELD, ids);
            return this;
        }

        private IdQualifierBuilder setIds(Character... ids) {
            this.map.put(MULTIPLE_IDS_FIELD, ids);
            return this;
        }

        private IdQualifierBuilder setIds(Byte... ids) {
            this.map.put(MULTIPLE_IDS_FIELD, ids);
            return this;
        }

        private IdQualifierBuilder setIds(byte[]... ids) {
            this.map.put(MULTIPLE_IDS_FIELD, ids);
            return this;
        }
    }

    private static class ConjunctionQualifierBuilder extends BaseQualifierBuilder<ConjunctionQualifierBuilder> {

        private ConjunctionQualifierBuilder() {
        }

        private ConjunctionQualifierBuilder setQualifiers(Qualifier... qualifiers) {
            this.map.put(QUALIFIERS, qualifiers);
            return this;
        }

        private Qualifier[] getQualifiers() {
            return (Qualifier[]) this.map.get(QUALIFIERS);
        }

        @Override
        protected void validate() {
            Assert.notNull(this.getQualifiers(), "Qualifiers must not be null");
            Assert.notEmpty(this.getQualifiers(), "Qualifiers must not be empty");
            Assert.isTrue(this.getQualifiers().length > 1, "There must be at least 2 qualifiers");
        }
    }

    @SuppressWarnings("unchecked")
    protected abstract static class BaseQualifierBuilder<T extends BaseQualifierBuilder<?>> implements Builder {

        protected final Map<String, Object> map = new HashMap<>();

        public FilterOperation getFilterOperation() {
            return (FilterOperation) this.map.get(OPERATION);
        }

        public T setFilterOperation(FilterOperation filterOperation) {
            this.map.put(OPERATION, filterOperation);
            return (T) this;
        }

        public String getField() {
            return (String) this.map.get(FIELD);
        }

        public boolean hasValue1() {
            return this.map.get(KEY) != null;
        }

        public boolean hasValue2() {
            return this.map.get(VALUE) != null;
        }

        public boolean hasDotPath() {
            return this.map.get(DOT_PATH) != null;
        }

        public Qualifier build() {
            validate();
            return new Qualifier(this);
        }

        public Map<String, Object> getMap() {
            return Collections.unmodifiableMap(this.map);
        }

        protected void validate() {
            // do nothing
        }
    }

    /**
     * Create a qualifier for the condition when the primary key equals the given string
     *
     * @param id String value
     * @return Single id qualifier
     */
    public static Qualifier idEquals(String id) {
        return new Qualifier(new IdQualifierBuilder()
            .setId(id)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals the given short
     *
     * @param id Short value
     * @return Single id qualifier
     */
    public static Qualifier idEquals(Short id) {
        return new Qualifier(new IdQualifierBuilder()
            .setId(id)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals the given integer
     *
     * @param id Integer value
     * @return Single id qualifier
     */
    public static Qualifier idEquals(Integer id) {
        return new Qualifier(new IdQualifierBuilder()
            .setId(id)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals the given long
     *
     * @param id Long value
     * @return Single id qualifier
     */
    public static Qualifier idEquals(Long id) {
        return new Qualifier(new IdQualifierBuilder()
            .setId(id)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals the given character
     *
     * @param id Character value
     * @return Single id qualifier
     */
    public static Qualifier idEquals(Character id) {
        return new Qualifier(new IdQualifierBuilder()
            .setId(id)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals the given byte
     *
     * @param id Byte value
     * @return Single id qualifier
     */
    public static Qualifier idEquals(Byte id) {
        return new Qualifier(new IdQualifierBuilder()
            .setId(id)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals the given byte array
     *
     * @param id Byte array value
     * @return Single id qualifier
     */
    public static Qualifier idEquals(byte[] id) {
        return new Qualifier(new IdQualifierBuilder()
            .setId(id)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals one of the given strings (logical OR)
     *
     * @param ids String values
     * @return Multiple ids qualifier with OR condition
     */
    public static Qualifier idIn(String... ids) {
        return new Qualifier(new IdQualifierBuilder()
            .setIds(ids)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals one of the given shorts (logical OR)
     *
     * @param ids Short values
     * @return Multiple ids qualifier with OR condition
     */
    public static Qualifier idIn(Short... ids) {
        return new Qualifier(new IdQualifierBuilder()
            .setIds(ids)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals one of the given integers (logical OR)
     *
     * @param ids Integer values
     * @return Multiple ids qualifier with OR condition
     */
    public static Qualifier idIn(Integer... ids) {
        return new Qualifier(new IdQualifierBuilder()
            .setIds(ids)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals one of the given longs (logical OR)
     *
     * @param ids Long values
     * @return Multiple ids qualifier with OR condition
     */
    public static Qualifier idIn(Long... ids) {
        return new Qualifier(new IdQualifierBuilder()
            .setIds(ids)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals one of the given characters (logical OR)
     *
     * @param ids Character values
     * @return Multiple ids qualifier with OR condition
     */
    public static Qualifier idIn(Character... ids) {
        return new Qualifier(new IdQualifierBuilder()
            .setIds(ids)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals one of the given bytes (logical OR)
     *
     * @param ids Byte values
     * @return Multiple ids qualifier with OR condition
     */
    public static Qualifier idIn(Byte... ids) {
        return new Qualifier(new IdQualifierBuilder()
            .setIds(ids)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a qualifier for the condition when the primary key equals one of the given byte arrays (logical OR)
     *
     * @param ids Byte array values
     * @return Multiple ids qualifier with OR condition
     */
    public static Qualifier idIn(byte[]... ids) {
        return new Qualifier(new IdQualifierBuilder()
            .setIds(ids)
            .setFilterOperation(FilterOperation.EQ));
    }

    /**
     * Create a parent qualifier that contains the given qualifiers combined using logical OR
     *
     * @param qualifiers Two or more qualifiers
     * @return Parent qualifier
     */
    public static Qualifier or(Qualifier... qualifiers) {
        return new Qualifier(new ConjunctionQualifierBuilder()
            .setFilterOperation(FilterOperation.OR)
            .setQualifiers(qualifiers));
    }

    /**
     * Create a parent qualifier that contains the given qualifiers combined using logical AND
     *
     * @param qualifiers Two or more qualifiers
     * @return Parent qualifier
     */
    public static Qualifier and(Qualifier... qualifiers) {
        return new Qualifier(new ConjunctionQualifierBuilder()
            .setFilterOperation(FilterOperation.AND)
            .setQualifiers(qualifiers));
    }
}
