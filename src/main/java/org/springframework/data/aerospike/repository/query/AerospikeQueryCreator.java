/*
 * Copyright 2012-2018 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapCriteria;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.TypeInformation;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreator extends 	AbstractQueryCreator<Query, AerospikeCriteria> {

	private static final Logger LOG = LoggerFactory.getLogger(AerospikeQueryCreator.class);
	private final MappingContext<?, AerospikePersistentProperty> context;

	public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters) {
		super(tree, parameters);
		this.context = new AerospikeMappingContext();
	}

	public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters,
								 MappingContext<?, AerospikePersistentProperty> context) {
		super(tree, parameters);
		this.context = context;
	}

	@Override
	protected AerospikeCriteria create(Part part, Iterator<Object> iterator) {
		PersistentPropertyPath<AerospikePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		AerospikePersistentProperty property = path.getLeafProperty();
		return create(part, property, iterator);
	}

	private AerospikeCriteria create(Part part, AerospikePersistentProperty property, Iterator<?> parameters){
		String fieldName = property.getFieldName();
		IgnoreCaseType ignoreCase = part.shouldIgnoreCase();
		FilterOperation op;
		Object v1 = parameters.next(), v2 = null, v3 = null;
		Qualifier.QualifierBuilder qb = new Qualifier.QualifierBuilder();

		switch (part.getType()) {
		case AFTER:
		case GREATER_THAN:
			op = FilterOperation.GT; break;
		case GREATER_THAN_EQUAL:
			op = FilterOperation.GTEQ; break;
		case BEFORE:
		case LESS_THAN:
			op = FilterOperation.LT; break;
		case LESS_THAN_EQUAL:
			op = FilterOperation.LTEQ; break;
		case BETWEEN:
			op = FilterOperation.BETWEEN;
			v2 = parameters.next();
			break;
		case LIKE:
		case STARTING_WITH:
			op = FilterOperation.STARTS_WITH;
			break;
		case ENDING_WITH:
			op = FilterOperation.ENDS_WITH; break;
		case CONTAINING:
			op = FilterOperation.CONTAINING; break;
		case WITHIN:
			op = FilterOperation.GEO_WITHIN;
			v1 = Value.get(String.format("{ \"type\": \"AeroCircle\", \"coordinates\": [[%.8f, %.8f], %f] }",
					  v1, parameters.next(), parameters.next()));
			break;
		case SIMPLE_PROPERTY:
			op = FilterOperation.EQ; break;
		case NEGATING_SIMPLE_PROPERTY:
			op = FilterOperation.NOTEQ; break;
		case IN:
			op = FilterOperation.IN; break;
		default:
			throw new IllegalArgumentException("Unsupported keyword!");
		}

		// TODO: further refactoring
		// Customization for collection/map query
		TypeInformation<?> propertyType = property.getTypeInformation();
		if (propertyType.isCollectionLike()) {
			switch (op) {
				case CONTAINING:
					op = FilterOperation.LIST_CONTAINS;
					break;
				case BETWEEN:
					op = FilterOperation.LIST_VALUE_BETWEEN;
					break;
				case GT:
					op = FilterOperation.LIST_VALUE_GT;
					break;
				case GTEQ:
					op = FilterOperation.LIST_VALUE_GTEQ;
					break;
				case LT:
					op = FilterOperation.LIST_VALUE_LT;
					break;
				case LTEQ:
					op = FilterOperation.LIST_VALUE_LTEQ;
					break;
			}
		} else {
			if (propertyType.isMap()) {
				if (parameters.hasNext()) {
					Object next = parameters.next();

					switch (op) {
						case EQ:
							op = FilterOperation.MAP_VALUE_EQ_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case NOTEQ:
							op = FilterOperation.MAP_VALUE_NOTEQ_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case GT:
							op = FilterOperation.MAP_VALUE_GT_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case GTEQ:
							op = FilterOperation.MAP_VALUE_GTEQ_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case LT:
							op = FilterOperation.MAP_VALUE_LT_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case LTEQ:
							op = FilterOperation.MAP_VALUE_LTEQ_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case STARTS_WITH:
							op = FilterOperation.MAP_VALUE_STARTS_WITH_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case ENDS_WITH:
							op = FilterOperation.MAP_VALUE_ENDS_WITH_BY_KEY;
							setQbValuesForMapByKey(qb, v1, next);
							break;
						case CONTAINING:
							if (next instanceof AerospikeMapCriteria) {
								AerospikeMapCriteria onMap = (AerospikeMapCriteria) next;
								switch (onMap) {
									case KEY:
										op = FilterOperation.MAP_KEYS_CONTAINS;
										break;
									case VALUE:
										op = FilterOperation.MAP_VALUES_CONTAINS;
										break;
								}
							} else {
								op = FilterOperation.MAP_VALUE_CONTAINING_BY_KEY;
								setQbValuesForMapByKey(qb, v1, next);
							}
							break;
						case BETWEEN:
							op = FilterOperation.MAP_VALUES_BETWEEN_BY_KEY;
							qb.setValue2(Value.get(v1)); // contains key
							qb.setValue1(Value.get(v2)); // contains lower limit (inclusive)
							qb.setValue3(Value.get(next)); // contains upper limit (inclusive)
							break;
					}
				} else {
					throw new AerospikeException("Not enough parameters (propertyType: Map, filterOperation: " + op + ")");
				}
			} else { // if it is neither a collection nor a map
				if (part.getProperty().hasNext() && isPojoField(part, property)) { // if it is a POJO field
					switch (op) {
						case EQ:
							op = FilterOperation.MAP_VALUE_EQ_BY_KEY;
							break;
						case NOTEQ:
							op = FilterOperation.MAP_VALUE_NOTEQ_BY_KEY;
							break;
						case GT:
							op = FilterOperation.MAP_VALUE_GT_BY_KEY;
							break;
						case GTEQ:
							op = FilterOperation.MAP_VALUE_GTEQ_BY_KEY;
							break;
						case LT:
							op = FilterOperation.MAP_VALUE_LT_BY_KEY;
							break;
						case LTEQ:
							op = FilterOperation.MAP_VALUE_LTEQ_BY_KEY;
							break;
						case BETWEEN:
							op = FilterOperation.MAP_VALUES_BETWEEN_BY_KEY;
							qb.setValue3(Value.get(v2)); // contains upper limit
							break;
						case STARTS_WITH:
							op = FilterOperation.MAP_VALUE_STARTS_WITH_BY_KEY;
							break;
						case ENDS_WITH:
							op = FilterOperation.MAP_VALUE_ENDS_WITH_BY_KEY;
							break;
						case CONTAINING:
							op = FilterOperation.MAP_VALUE_CONTAINING_BY_KEY;
							break;
						default:
							break;
					}

					fieldName = part.getProperty().getSegment(); // POJO name, later passed to Exp.mapBin()
					qb.setValue2(Value.get(property.getFieldName())); // VALUE2 contains key (field name)
				}
			}
		}

		qb.setIgnoreCase(true)
			.setField(fieldName)
			.setFilterOperation(op);
		setNotNullQbValues(qb, v1, v2);

		return new AerospikeCriteria(qb);
	}

	private void setNotNullQbValues(Qualifier.QualifierBuilder qb, Object v1, Object v2) {
		if (! qb.hasValue1() && v1 != null) qb.setValue1(Value.get(v1));
		if (! qb.hasValue2() && v2 != null) qb.setValue2(Value.get(v2));
	}

	private void setQbValuesForMapByKey(Qualifier.QualifierBuilder qb, Object key, Object value) {
		qb.setValue1(Value.get(value)); // contains value
		qb.setValue2(Value.get(key)); // contains key
	}

	private boolean isPojoField(Part part, AerospikePersistentProperty property) {
		return Arrays.stream(part.getProperty().getType().getDeclaredFields())
				.anyMatch(f -> f.getName().equals(property.getName()));
	}

	@Override
	protected AerospikeCriteria and(Part part, AerospikeCriteria base, Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<AerospikePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		AerospikePersistentProperty property = path.getLeafProperty();

		return new AerospikeCriteria(new Qualifier.QualifierBuilder()
				.setFilterOperation(FilterOperation.AND)
				.setQualifiers(base, create(part, property, iterator))
		);
	}

	@Override
	protected AerospikeCriteria or(AerospikeCriteria base, AerospikeCriteria criteria) {
		return new AerospikeCriteria(new Qualifier.QualifierBuilder()
				.setFilterOperation(FilterOperation.OR)
				.setQualifiers(base, criteria)
		);
	}

	@Override
	protected Query complete(AerospikeCriteria criteria, Sort sort) {
		Query query = criteria == null ? null : new Query(criteria).with(sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + query);
		}

		return query;
	}

	@SuppressWarnings("unused")
	private boolean isSimpleComparisonPossible(Part part) {
		switch (part.shouldIgnoreCase()) {
			case WHEN_POSSIBLE:
				return part.getProperty().getType() != String.class;
			case ALWAYS:
				return false;
			case NEVER:
			default:
				return true;
		}
	}
}
