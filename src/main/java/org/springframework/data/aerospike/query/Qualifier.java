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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Generic Bin qualifier. It acts as a filter to exclude records that do not meet the given criteria.
 * <p>
 * For the list of the supported operations see
 * {@link FilterOperation}
 *
 * @author Peter Milne
 */
public class Qualifier implements Map<String, Object>, Serializable {
	private static final long serialVersionUID = -2689196529952712849L;
	protected static final String FIELD = "field";
	protected static final String IGNORE_CASE = "ignoreCase";
	protected static final String VALUE1 = "value1";
	protected static final String VALUE2 = "value2";
	protected static final String VALUE3 = "value3";
	protected static final String QUALIFIERS = "qualifiers";
	protected static final String OPERATION = "operation";
	protected static final String AS_FILTER = "queryAsFilter";
	protected Map<String, Object> internalMap;

	public static class QualifierRegexpBuilder {
		private static final Character BACKSLASH = '\\';
		private static final Character DOT = '.';
		private static final Character ASTERISK = '*';
		private static final Character DOLLAR = '$';
		private static final Character OPEN_BRACKET = '[';
		private static final Character CIRCUMFLEX = '^';

		public static String escapeBRERegexp(String base) {
			StringBuilder builder = new StringBuilder();
			for (char stringChar : base.toCharArray()) {
				if (
						stringChar == BACKSLASH ||
								stringChar == DOT ||
								stringChar == ASTERISK ||
								stringChar == DOLLAR ||
								stringChar == OPEN_BRACKET ||
								stringChar == CIRCUMFLEX) {
					builder.append(BACKSLASH);
				}
				builder.append(stringChar);
			}
			return builder.toString();
		}

		/*
		 * This op is always in [START_WITH, ENDS_WITH, EQ, CONTAINING]
		 */
		private static String getRegexp(String base, FilterOperation op) {
			String escapedBase = escapeBRERegexp(base);
			if (op == FilterOperation.STARTS_WITH) {
				return "^" + escapedBase;
			}
			if (op == FilterOperation.ENDS_WITH) {
				return escapedBase + "$";
			}
			if (op == FilterOperation.EQ) {
				return "^" + escapedBase + "$";
			}
			return escapedBase;
		}

		public static String getStartsWith(String base) {
			return getRegexp(base, FilterOperation.STARTS_WITH);
		}

		public static String getEndsWith(String base) {
			return getRegexp(base, FilterOperation.ENDS_WITH);
		}

		public static String getContaining(String base) {
			return getRegexp(base, FilterOperation.CONTAINING);
		}

		public static String getStringEquals(String base) {
			return getRegexp(base, FilterOperation.EQ);
		}
	}

	public Qualifier(QualifierBuilder builder) {
		internalMap = new HashMap<>();

		if (! builder.buildMap().isEmpty()) {
			internalMap.putAll(builder.buildMap());
		}
	}

	public static class QualifierBuilder {
		private final Map<String, Object> map = new HashMap<>();

		public QualifierBuilder() {
		}

		public QualifierBuilder setField(String field) {
			this.map.put(FIELD, field);
			return this;
		}

		public QualifierBuilder setIgnoreCase(boolean ignoreCase) {
			this.map.put(IGNORE_CASE, ignoreCase);
			return this;
		}

		public QualifierBuilder setFilterOperation(FilterOperation filterOperation) {
			this.map.put(OPERATION, filterOperation);
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

		public Qualifier build() {
			return new Qualifier(this);
		}

		public Map<String, Object> buildMap() {
			return this.map;
		}
	}

	public FilterOperation getOperation() {
		return (FilterOperation) internalMap.get(OPERATION);
	}

	public String getField() {
		return (String) internalMap.get(FIELD);
	}

	public void asFilter(Boolean queryAsFilter) {
		internalMap.put(AS_FILTER, queryAsFilter);
	}

	public Boolean queryAsFilter() {
		return internalMap.containsKey(AS_FILTER) && (Boolean) internalMap.get(AS_FILTER);
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

	public Value getValue3() {
		return (Value) internalMap.get(VALUE3);
	}

	public Filter asFilter() {
		FilterOperation op = getOperation();
		switch (op) {
			case EQ:
				if (getValue1().getType() == ParticleType.INTEGER) {
					return Filter.equal(getField(), getValue1().toLong());
				} else {
					// There is no case insensitive string comparison filter.
					if (ignoreCase()) {
						return null;
					}
					return Filter.equal(getField(), getValue1().toString());
				}
			case GTEQ:
			case BETWEEN:
				return Filter.range(getField(), getValue1().toLong(), getValue2() == null ? Long.MAX_VALUE : getValue2().toLong());
			case GT:
				return Filter.range(getField(), getValue1().toLong() + 1, getValue2() == null ? Long.MAX_VALUE : getValue2().toLong());
			case LT:
				return Filter.range(getField(), Long.MIN_VALUE, getValue1().toLong() - 1);
			case LTEQ:
				return Filter.range(getField(), Long.MIN_VALUE, getValue1().toLong());
			case LIST_CONTAINS:
				return collectionContains(IndexCollectionType.LIST);
			case MAP_KEYS_CONTAINS:
				return collectionContains(IndexCollectionType.MAPKEYS);
			case MAP_VALUES_CONTAINS:
				return collectionContains(IndexCollectionType.MAPVALUES);
			case LIST_VALUE_BETWEEN:
				return collectionRange(IndexCollectionType.LIST);
			case MAP_KEYS_BETWEEN:
				return collectionRange(IndexCollectionType.MAPKEYS);
			case MAP_VALUES_BETWEEN:
				return collectionRange(IndexCollectionType.MAPVALUES);
			case GEO_WITHIN:
				return geoWithinRadius(IndexCollectionType.DEFAULT);
			default:
				return null;
		}
	}

	private Filter geoWithinRadius(IndexCollectionType collectionType) {
		return Filter.geoContains(getField(), getValue1().toString());
	}

	private Filter collectionContains(IndexCollectionType collectionType) {
		Value val = getValue1();
		int valType = val.getType();
		switch (valType) {
			case ParticleType.INTEGER:
				return Filter.contains(getField(), collectionType, val.toLong());
			case ParticleType.STRING:
				return Filter.contains(getField(), collectionType, val.toString());
		}
		return null;
	}

	private Filter collectionRange(IndexCollectionType collectionType) {
		return Filter.range(getField(), collectionType, getValue1().toLong(), getValue2().toLong());
	}

	public Exp toFilterExp() {
		try {
			return FilterOperation.valueOf(getOperation().toString()).process(internalMap);
		} catch (Exception e) {
			throw new AerospikeException("FilterExpression unsupported operation: " + getOperation());
		}
	}

	private Boolean ignoreCase() {
		return (Boolean) internalMap.getOrDefault(IGNORE_CASE, false);
	}

	protected String luaFieldString(String field) {
		return String.format("rec['%s']", field);
	}

	protected String luaValueString(Value value) {
		String res = null;
		if (null == value) return res;
		int type = value.getType();
		switch (type) {
			//		case ParticleType.LIST:
			//			res = value.toString();
			//			break;
			//		case ParticleType.MAP:
			//			res = value.toString();
			//			break;
			//		case ParticleType.DOUBLE:
			//			res = value.toString();
			//			break;
			case ParticleType.STRING:
				res = String.format("'%s'", value.toString());
				break;
			case ParticleType.GEOJSON:
				res = String.format("'%s'", value.toString());
				break;
			default:
				res = value.toString();
				break;
		}
		return res;
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
	public void putAll(Map<? extends String, ? extends Object> m) {
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
		return String.format("%s:%s:%s:%s", getField(), getOperation(), getValue1(), getValue2());
	}
}
