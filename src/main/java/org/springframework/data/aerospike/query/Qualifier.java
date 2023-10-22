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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Generic Bin qualifier. It acts as a filter to exclude records that do not meet the given criteria.
 * <p>
 * For the list of the supported operations see {@link FilterOperation}
 *
 * @author Peter Milne
 */
public class Qualifier implements Map<String, Object>, Serializable {

    protected static final String FIELD = "field";
    protected static final String METADATA_FIELD = "metadata_field";
    protected static final String ID_VALUE = "id";
    protected static final String IGNORE_CASE = "ignoreCase";
    protected static final String VALUE1 = "value1";
    protected static final String VALUE2 = "value2";
    protected static final String VALUE3 = "value3";
    protected static final String DOT_PATH = "dotPath";
    protected static final String CONVERTER = "converter";
    protected static final String QUALIFIERS = "qualifiers";
    protected static final String OPERATION = "operation";
    protected static final String AS_FILTER = "queryAsFilter";
    protected static final String EXCLUDE_FILTER = "excludeFilter";
    @Serial
    private static final long serialVersionUID = -2689196529952712849L;
    protected final Map<String, Object> internalMap;

    protected Qualifier(Qualifier.Builder builder) {
        internalMap = new HashMap<>();

        if (!builder.buildMap().isEmpty()) {
            internalMap.putAll(builder.buildMap());
        }
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

    public boolean getExcludeFilter() {
        return internalMap.containsKey(EXCLUDE_FILTER) && (Boolean) internalMap.get(EXCLUDE_FILTER);
    }

    public void setExcludeFilter(boolean excludeFilter) {
        internalMap.put(EXCLUDE_FILTER, excludeFilter);
    }

    public boolean hasQualifiers() {
        return internalMap.get(QUALIFIERS) != null;
    }

    public boolean hasId() {
        return internalMap.get(FIELD) != null && internalMap.get(FIELD).equals(ID_VALUE);
    }

    public Qualifier[] getQualifiers() {
        return (Qualifier[]) internalMap.get(QUALIFIERS);
    }

    public Value getValue1() {
        return (Value) internalMap.get(VALUE1);
    }

    public Value getValue2() {
        return (Value) internalMap.get(VALUE2);
    }

    public String getDotPath() {
        return (String) internalMap.get(DOT_PATH);
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
            return String.format("%s:%s:%s:%s", getField(), getOperation(), getValue1(), getValue2());
        }
        return String.format("(metadata)%s:%s:%s:%s", getMetadataField().toString(),
            getOperation(), getValue1(), getValue2());
    }

    public interface Builder {

        Map<String, Object> buildMap();

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

        public QualifierBuilder setQualifiers(Qualifier... qualifiers) {
            this.map.put(QUALIFIERS, qualifiers);
            return this;
        }

        public QualifierBuilder setValue1(Value value1) {
            this.map.put(VALUE1, value1);
            return this;
        }

        public QualifierBuilder setValue2(Value value2) {
            this.map.put(VALUE2, value2);
            return this;
        }

        public QualifierBuilder setValue3(Value value3) {
            this.map.put(VALUE3, value3);
            return this;
        }

        public boolean hasValue3() {
            return this.map.get(VALUE3) != null;
        }

        public void setDotPath(String dotPath) {
            this.map.put(DOT_PATH, dotPath);
        }

        public QualifierBuilder setConverter(MappingAerospikeConverter converter) {
            this.map.put(CONVERTER, converter);
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

        public Object getValue1AsObj() {
            return this.map.get(VALUE1);
        }

        public MetadataQualifierBuilder setValue1AsObj(Object object) {
            this.map.put(VALUE1, object);
            return this;
        }

        public Object getValue2AsObj() {
            return this.map.get(VALUE2);
        }

        public MetadataQualifierBuilder setValue2AsObj(Object object) {
            this.map.put(VALUE2, object);
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
                        case EQ, NOTEQ, LT, LTEQ, GT, GTEQ -> Assert.isTrue(this.getValue1AsObj() instanceof Long,
                            operation.name() + ": value1 is expected to be set as Long");
                        case BETWEEN -> {
                            Assert.isTrue(this.getValue1AsObj() instanceof Long,
                                "BETWEEN: value1 is expected to be set as Long");
                            Assert.isTrue(this.getValue2AsObj() instanceof Long,
                                "BETWEEN: value2 is expected to be set as Long");
                        }
                        case NOT_IN, IN -> Assert.isTrue(this.getValue1AsObj() instanceof Collection
                                && (!((Collection<Object>) this.getValue1AsObj()).isEmpty())
                                && ((Collection<Object>) this.getValue1AsObj()).toArray()[0] instanceof Long,
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
            return this.map.get(VALUE1) != null;
        }

        public boolean hasValue2() {
            return this.map.get(VALUE2) != null;
        }

        public boolean hasDotPath() {
            return this.map.get(DOT_PATH) != null;
        }

        public Qualifier build() {
            validate();
            return new Qualifier(this);
        }

        public Map<String, Object> buildMap() {
            return this.map;
        }

        protected void validate() {
            // do nothing
        }
    }

    /**
     * @param id String value
     * @return Qualifier with the condition "Id equal to the given String"
     */
    public static Qualifier forId(String id) {
        return new Qualifier(new QualifierBuilder()
            .setField(ID_VALUE)
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(id)));
    }

    /**
     * @param ids String values
     * @return Qualifier with the condition "Id equal to one of the given Strings (logical OR)"
     */
    public static Qualifier forIds(String... ids) {
        return new Qualifier(new QualifierBuilder()
            .setField(ID_VALUE)
            .setFilterOperation(FilterOperation.EQ)
            .setValue1(Value.get(Arrays.stream(ids).toList())));
    }
}
