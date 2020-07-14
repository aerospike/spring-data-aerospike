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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.CachingAerospikePersistentProperty;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapCriteria;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.PartTree;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.query.Qualifier.FilterOperation;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class AerospikeQueryCreator extends 	AbstractQueryCreator<Query, AerospikeCriteria> {

	private static final Logger LOG = LoggerFactory.getLogger(AerospikeQueryCreator.class);
	private MappingContext<?, AerospikePersistentProperty> context;

	/**
	 * @param tree
	 * @param parameters
	 */
	public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters) {
		super(tree, parameters);
		this.context = new AerospikeMappingContext();
	}

	/**
	 * @param tree
	 * @param parameters
	 */
	public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters,MappingContext<?, AerospikePersistentProperty> context) {
		super(tree, parameters);
		this.context = context;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
	 */
	@Override
	protected AerospikeCriteria create(Part part, Iterator<Object> iterator) {
		PersistentPropertyPath<AerospikePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		AerospikePersistentProperty property = path.getLeafProperty();
		return create(part, property, iterator);
	}
	private AerospikeCriteria create(Part part, AerospikePersistentProperty property, Iterator<?> parameters){
		String fieldName = ((CachingAerospikePersistentProperty) property).getFieldName();
		IgnoreCaseType ignoreCase = part.shouldIgnoreCase();
		FilterOperation op;
		Object v1 = parameters.next(), v2 = null;
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
			op = FilterOperation.START_WITH; break;
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
		
		//customization for collection/map query
		if(property instanceof Collection){
			if(op==FilterOperation.CONTAINING) op = FilterOperation.LIST_CONTAINS;
			else if(op == FilterOperation.BETWEEN) op = FilterOperation.LIST_BETWEEN;
		}else if (property instanceof Map){
			AerospikeMapCriteria onMap = (AerospikeMapCriteria) parameters.next();
			switch (onMap){
			case KEY:  if(op==FilterOperation.CONTAINING) op = FilterOperation.MAP_KEYS_CONTAINS; break;
			case VALUE: if(op==FilterOperation.CONTAINING) op = FilterOperation.MAP_VALUES_CONTAINS; break;
			}
		}
		if(null == v2)return new AerospikeCriteria(fieldName, op,  ignoreCase==IgnoreCaseType.ALWAYS, Value.get(v1));
		else return new AerospikeCriteria(fieldName, op, Value.get(v1), Value.get(v2));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
	 */
	@Override
	protected AerospikeCriteria and(Part part, AerospikeCriteria base, Iterator<Object> iterator) {
		if (base == null) {
			return create(part, iterator);
		}
		PersistentPropertyPath<AerospikePersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		AerospikePersistentProperty property = path.getLeafProperty();
		
		return new AerospikeCriteria(FilterOperation.AND, base, create(part, property, iterator));

		//return from(part, property, Criteria.where(path.toDotPath()), iterator);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#or(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected AerospikeCriteria or(AerospikeCriteria base, AerospikeCriteria criteria) {
		return new AerospikeCriteria(FilterOperation.OR, base, criteria);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#complete(java.lang.Object, org.springframework.data.domain.Sort)
	 */
	@Override
	protected Query complete(AerospikeCriteria criteria, Sort sort) {
		Query query = (criteria == null ? null : new Query(criteria)).with(sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + query);
		}

		return query;
	}

	@SuppressWarnings("unused")
	private boolean isSimpleComparisionPossible(Part part) {
		switch (part.shouldIgnoreCase()) {
			case NEVER:
				return true;
			case WHEN_POSSIBLE:
				return part.getProperty().getType() != String.class;
			case ALWAYS:
				return false;
			default:
				return true;
		}
	}
}
