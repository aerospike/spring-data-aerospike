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
import com.aerospike.client.cdt.ListReturnType;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ListExp;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.RegexFlag;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
//	private static final String listIterVar = "listIterVar";
//	private static final String mapIterVar = "mapIterVar";
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
			if (op == FilterOperation.START_WITH) {
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
			return getRegexp(base, FilterOperation.START_WITH);
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

	public enum FilterOperation {
		EQ, GT, GTEQ, LT, LTEQ, NOTEQ, BETWEEN, START_WITH, ENDS_WITH, CONTAINING, IN,
		LIST_CONTAINS, MAP_KEYS_CONTAINS, MAP_VALUES_CONTAINS,
		MAP_VALUE_EQ_BY_KEY, MAP_VALUE_NOTEQ_BY_KEY, MAP_VALUE_GT_BY_KEY, MAP_VALUE_GTEQ_BY_KEY,
		MAP_VALUE_LT_BY_KEY, MAP_VALUE_LTEQ_BY_KEY, MAP_VALUES_BETWEEN_BY_KEY,
		MAP_VALUE_START_WITH_BY_KEY, MAP_VALUE_ENDS_WITH_BY_KEY, MAP_VALUE_CONTAINING_BY_KEY,
		LIST_BETWEEN, MAP_KEYS_BETWEEN, MAP_VALUES_BETWEEN, GEO_WITHIN,
		OR, AND
	}

	public Qualifier() {
		super();
		internalMap = new HashMap<>();
	}

	public Qualifier(FilterOperation operation, Qualifier... qualifiers) {
		this();
		internalMap.put(QUALIFIERS, qualifiers);
		internalMap.put(OPERATION, operation);
	}

	public Qualifier(String field, FilterOperation operation, Value value1) {
		this(field, operation, Boolean.FALSE, value1);
	}

	public Qualifier(String field, FilterOperation operation, Boolean ignoreCase, Value value1) {
		this();
		internalMap.put(FIELD, field);
		internalMap.put(OPERATION, operation);
		internalMap.put(VALUE1, value1);
		internalMap.put(IGNORE_CASE, ignoreCase);
	}

	public Qualifier(String field, FilterOperation operation, Value value1, Value value2) {
		this(field, operation, Boolean.FALSE, value1);
		internalMap.put(VALUE2, value2);
	}

	public Qualifier(String field, FilterOperation operation, Value value1, Value value2, Value value3) {
		this(field, operation, Boolean.FALSE, value1);
		internalMap.put(VALUE2, value2);
		internalMap.put(VALUE3, value3);
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
			case LIST_BETWEEN:
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
		int regexFlags = ignoreCase() ? RegexFlag.ICASE : RegexFlag.NONE;
		Exp exp;
		switch (getOperation()) {
			case AND:
				Qualifier[] qs = (Qualifier[]) get(QUALIFIERS);
				Exp[] childrenExp = new Exp[qs.length];
				for (int i = 0; i < qs.length; i++) {
					childrenExp[i] = qs[i].toFilterExp();
				}
				exp = Exp.and(childrenExp);
				break;
			case OR:
				qs = (Qualifier[]) get(QUALIFIERS);
				childrenExp = new Exp[qs.length];
				for (int i = 0; i < qs.length; i++) {
					childrenExp[i] = qs[i].toFilterExp();
				}
				exp = Exp.or(childrenExp);
				break;
			case IN: // Convert IN to a collection of or as Aerospike has not support for IN query
				Value val = getValue1();
				int valType = val.getType();
				if (valType != ParticleType.LIST)
					throw new IllegalArgumentException("FilterOperation.IN expects List argument with type: " + ParticleType.LIST + ", but got: " + valType);
				List<?> inList = (List<?>) val.getObject();
				Exp[] listElementsExp = new Exp[inList.size()];

				for (int i = 0; i < inList.size(); i++) {
					listElementsExp[i] = new Qualifier(this.getField(), FilterOperation.EQ, Value.get(inList.get(i))).toFilterExp();
				}
				exp = Exp.or(listElementsExp);
				break;
			case EQ:
				val = getValue1();
				valType = val.getType();
				switch (valType) {
					case ParticleType.INTEGER:
						exp = Exp.eq(Exp.intBin(getField()), Exp.val(val.toLong()));
						break;
					case ParticleType.STRING:
						if (ignoreCase()) {
							String equalsRegexp = QualifierRegexpBuilder.getStringEquals(getValue1().toString());
							exp = Exp.regexCompare(equalsRegexp, RegexFlag.ICASE, Exp.stringBin(getField()));
						} else {
							exp = Exp.eq(Exp.stringBin(getField()), Exp.val(val.toString()));
						}
						break;
					default:
						throw new AerospikeException("FilterExpression unsupported particle type: " + valType);
				}
				break;
			case NOTEQ:
				val = getValue1();
				valType = val.getType();
				if (valType == ParticleType.INTEGER) {
					exp = Exp.ne(Exp.intBin(getField()), Exp.val(val.toLong()));
				} else {
					exp = Exp.ne(Exp.stringBin(getField()), Exp.val(val.toString()));
				}
				break;
			case GT:
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.gt(Exp.intBin(getField()), Exp.val(getValue1().toLong()));
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case GTEQ:
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.ge(Exp.intBin(getField()), Exp.val(getValue1().toLong()));
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case LT:
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.lt(Exp.intBin(getField()), Exp.val(getValue1().toLong()));
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case LTEQ:
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.le(Exp.intBin(getField()), Exp.val(getValue1().toLong()));
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case BETWEEN:
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.and(Exp.ge(Exp.intBin(getField()), Exp.val(getValue1().toLong())), Exp.le(Exp.intBin(getField()), Exp.val(getValue2().toLong())));
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case START_WITH:
				String startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1().toString());
				exp = Exp.regexCompare(startWithRegexp, regexFlags, Exp.stringBin(getField()));
				break;
			case ENDS_WITH:
				String endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1().toString());
				exp = Exp.regexCompare(endWithRegexp, regexFlags, Exp.stringBin(getField()));
				break;
			case CONTAINING:
				String containingRegexp = QualifierRegexpBuilder.getContaining(getValue1().toString());
				exp = Exp.regexCompare(containingRegexp, regexFlags, Exp.stringBin(getField()));
				break;
			case MAP_VALUE_EQ_BY_KEY:
				// VALUE2 contains key (field name)
				switch (getValue1().getType()) {
					case ParticleType.STRING:
						exp = Exp.eq(
								MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
								Exp.val(getValue1().toString())
						);
						break;
					case ParticleType.INTEGER:
						exp = Exp.eq(
								MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
								Exp.val(getValue1().toLong())
						);
						break;
					default:
						throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected String or Long)");
				}
				break;
			case MAP_VALUE_NOTEQ_BY_KEY:
				switch (getValue1().getType()) {
					case ParticleType.STRING:
						exp = Exp.ne(
								MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
								Exp.val(getValue1().toString())
						);
						break;
					case ParticleType.INTEGER:
						exp = Exp.ne(
								MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
								Exp.val(getValue1().toLong())
						);
						break;
					default:
						throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected String or Long)");
				}
				break;
			case MAP_VALUE_GT_BY_KEY:
				// VALUE2 contains key (field name)
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.gt(
							MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
							Exp.val(getValue1().toLong())
					);
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case MAP_VALUE_GTEQ_BY_KEY:
				// VALUE2 contains key (field name)
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.ge(
							MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
							Exp.val(getValue1().toLong())
					);
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case MAP_VALUE_LT_BY_KEY:
				// VALUE2 contains key (field name)
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.lt(
							MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
							Exp.val(getValue1().toLong())
					);
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case MAP_VALUE_LTEQ_BY_KEY:
				// VALUE2 contains key (field name)
				if (getValue1().getType() == ParticleType.INTEGER) {
					exp = Exp.le(
							MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
							Exp.val(getValue1().toLong())
					);
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case MAP_VALUES_BETWEEN_BY_KEY:
				// VALUE2 contains key (field name), VALUE3 contains upper limit
				if (getValue1().getType() == ParticleType.INTEGER && getValue3().getType() == ParticleType.INTEGER) {
					exp = Exp.and(
							Exp.ge(
									MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
									Exp.val(getValue1().toLong())
							),
							Exp.le(
									MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(getValue2().toString()), Exp.mapBin(getField())),
									Exp.val(getValue3().toLong())
							)
					);
					break;
				}
				throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected Long)");
			case MAP_VALUE_START_WITH_BY_KEY:
				startWithRegexp = QualifierRegexpBuilder.getStartsWith(getValue1().toString());
				exp = Exp.regexCompare(startWithRegexp, regexFlags,
						MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2().toString()), Exp.mapBin(getField()))
				);
				break;
			case MAP_VALUE_ENDS_WITH_BY_KEY:
				endWithRegexp = QualifierRegexpBuilder.getEndsWith(getValue1().toString());
				exp = Exp.regexCompare(endWithRegexp, regexFlags,
						MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2().toString()), Exp.mapBin(getField()))
				);
				break;
			case MAP_VALUE_CONTAINING_BY_KEY:
				containingRegexp = QualifierRegexpBuilder.getContaining(getValue1().toString());
				exp = Exp.regexCompare(containingRegexp, regexFlags,
						MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(getValue2().toString()), Exp.mapBin(getField()))
				);
				break;
			case GEO_WITHIN:
				exp = Exp.geoCompare(Exp.geoBin(getField()), Exp.geo(getValue1().toString()));
				break;
			case LIST_CONTAINS:
				switch (getValue1().getType()) {
					case ParticleType.STRING:
						exp = Exp.gt(
								ListExp.getByValue(ListReturnType.COUNT, Exp.val(getValue1().toString()), Exp.listBin(getField())),
								Exp.val(0)
						);
						break;
					case ParticleType.INTEGER:
						exp = Exp.gt(
								ListExp.getByValue(ListReturnType.COUNT, Exp.val(getValue1().toLong()), Exp.listBin(getField())),
								Exp.val(0)
						);
						break;
					default:
						throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected String or Long)");
				}
				break;
			case MAP_KEYS_CONTAINS:
				switch (getValue1().getType()) {
					case ParticleType.STRING:
						exp = Exp.gt(
								MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue1().toString()), Exp.mapBin(getField())),
								Exp.val(0)
						);
						break;
					case ParticleType.INTEGER:
						exp = Exp.gt(
								MapExp.getByKey(MapReturnType.COUNT, Exp.Type.INT, Exp.val(getValue1().toLong()), Exp.mapBin(getField())),
								Exp.val(0)
						);
						break;
					default:
						throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected String or Long)");
				}
				break;
			case MAP_VALUES_CONTAINS:
				switch (getValue1().getType()) {
					case ParticleType.STRING:
						exp = Exp.gt(
								MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue1().toString()), Exp.mapBin(getField())),
								Exp.val(0)
						);
						break;
					case ParticleType.INTEGER:
						exp = Exp.gt(
								MapExp.getByValue(MapReturnType.COUNT, Exp.val(getValue1().toLong()), Exp.mapBin(getField())),
								Exp.val(0)
						);
						break;
					default:
						throw new AerospikeException("FilterExpression unsupported operation: " + getOperation() + " (expected String or Long)");
				}
				break;
			case LIST_BETWEEN:
				exp = Exp.gt(
						// + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
						ListExp.getByValueRange(ListReturnType.COUNT, Exp.val(getValue1().toLong()), Exp.val(getValue2().toLong() + 1L), Exp.listBin(getField())),
						Exp.val(0)
				);
				break;
			case MAP_KEYS_BETWEEN:
				exp = Exp.gt(
						// + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
						MapExp.getByKeyRange(MapReturnType.COUNT, Exp.val(getValue1().toLong()), Exp.val(getValue2().toLong() + 1L), Exp.mapBin(getField())),
						Exp.val(0)
				);
				break;
			case MAP_VALUES_BETWEEN:
				exp = Exp.gt(
						// + 1L to the valueEnd since the valueEnd is exclusive (both begin and values should be included).
						MapExp.getByValueRange(MapReturnType.COUNT, Exp.val(getValue1().toLong()), Exp.val(getValue2().toLong() + 1L), Exp.mapBin(getField())),
						Exp.val(0)
				);
				break;
			default:
				throw new AerospikeException("FilterExpression Unsupported Operation: " + getOperation());
		}
		return exp;
	}

	private Boolean ignoreCase() {
		Boolean ignoreCase = (Boolean) internalMap.get(IGNORE_CASE);
		return ignoreCase == Boolean.TRUE;
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
				res = String.format("'%s'", value);
				break;
			case ParticleType.GEOJSON:
				res = String.format("'%s'", value);
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
		return String.format("%s:%s:%s:%s", getField(), getOperation(), getValue1(), getValue2());
	}
}
