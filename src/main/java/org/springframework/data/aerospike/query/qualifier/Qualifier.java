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
package org.springframework.data.aerospike.query.qualifier;

import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition;
import org.springframework.util.StringUtils;

import java.io.Serial;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.springframework.data.aerospike.query.qualifier.QualifierKey.*;

/**
 * Generic Bin qualifier. It acts as a filter to exclude records that do not meet the given criteria.
 * <p>
 * For the list of the supported operations see {@link FilterOperation}
 *
 * @author Peter Milne
 */
public class Qualifier implements CriteriaDefinition, Map<QualifierKey, Object>, Serializable {

    @Serial
    private static final long serialVersionUID = -2689196529952712849L;
    protected final Map<QualifierKey, Object> internalMap = new HashMap<>();

    protected Qualifier(IQualifierBuilder builder) {
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
    public String getCriteriaField() {
        return this.getField();
    }

    private Map<QualifierKey, Object> getMap() {
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

    public void setDataSettings(AerospikeDataSettings dataSettings) {
        internalMap.put(DATA_SETTINGS, dataSettings);
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
    public Object put(QualifierKey key, Object value) {
        return internalMap.put(key, value);
    }

    @Override
    public Object remove(Object key) {
        return internalMap.remove(key);
    }

    @Override
    public void putAll(Map<? extends QualifierKey, ?> m) {
        internalMap.putAll(m);
    }

    @Override
    public void clear() {
        internalMap.clear();
    }

    @Override
    public Set<QualifierKey> keySet() {
        return internalMap.keySet();
    }

    @Override
    public Collection<Object> values() {
        return internalMap.values();
    }

    @Override
    public Set<Entry<QualifierKey, Object>> entrySet() {
        return internalMap.entrySet();
    }

    @Override
    public String toString() {
        if (!StringUtils.hasLength(getField()) && StringUtils.hasLength(getMetadataField().toString())) {
            return String.format("%s:%s:%s:%s:%s", getField(), getOperation(), getKey(), getValue(), getSecondValue());
        }
        return String.format("(metadata) %s:%s:%s:%s:%s", getMetadataField().toString(),
            getOperation(), getKey(), getValue(), getSecondValue());
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
