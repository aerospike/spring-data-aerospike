/*
 * Copyright 2012-2020 the original author or authors
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

import com.aerospike.client.Value;
import org.springframework.data.aerospike.InvalidAerospikeDataAccessApiUsageException;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.Qualifier.FilterOperation;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class Criteria implements CriteriaDefinition {

	private static final Object NOT_SET = new Object();

	private String key;
	private final List<Criteria> criteriaChain;
	private final LinkedHashMap<String, Object> criteria = new LinkedHashMap<>();
	private Object isValue = NOT_SET;

	public Criteria(String key) {
		this.criteriaChain = new ArrayList<>();
		this.criteriaChain.add(this);
		this.key = key;
	}

	protected Criteria(List<Criteria> criteriaChain, String key) {
		this.criteriaChain = criteriaChain;
		this.criteriaChain.add(this);
		this.key = key;
	}

	public Criteria() {
		this.criteriaChain = new ArrayList<>();
	}

	@Override
	public Qualifier getCriteriaObject() {
		if (this.criteriaChain.size() == 1) {
			return criteriaChain.get(0).getSingleCriteriaObject();
		} else if (CollectionUtils.isEmpty(this.criteriaChain) && !CollectionUtils.isEmpty(this.criteria)){
			return getSingleCriteriaObject();
		} else {
			FilterOperation op = FilterOperation.valueOf(key);
			List<Qualifier> qualifiers = new ArrayList<>();
			for (Criteria c : this.criteriaChain) {
				qualifiers.add(c.getCriteriaObject());
			}
			return new Qualifier(op, qualifiers.toArray(new Qualifier[0]));
		}
	}

	protected Qualifier getSingleCriteriaObject() {
		Qualifier qualifier = null;
		for (String k : this.criteria.keySet()) {
			qualifier = (Qualifier) this.criteria.get(k);
		}

		return qualifier;
	}

	@Override
	public String getKey() {
		return this.key;
	}

	/**
	 * Static factory method to create a Criteria using the provided key.
	 *
	 * @param key the provided key
	 * @return the Criteria instance
	 */
	public static Criteria where(String key) {
		return new Criteria(key);
	}

	public Criteria and(String key) {
		return new Criteria(this.criteriaChain, key);
	}

	public Criteria gt(Object o, String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.GT, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.GT.name(), qualifier);
		return this;
	}

	public Criteria gte(Object o,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.GTEQ, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.GTEQ.name(), qualifier);
		return this;
	}

	public Criteria lt(Object o,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.LT, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.LT.name(), qualifier);
		return this;
	}

	public Criteria lte(Object o,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.LTEQ, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.LTEQ.name(), qualifier);
		return this;
	}

	public Criteria ne(Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	private boolean lastOperatorWasNot() {
		return this.criteria.size() > 0 && "$not".equals(
				this.criteria.keySet().toArray()[this.criteria.size() - 1]);
	}

	@SuppressWarnings("unused")
	private boolean lastOperatorWasNotEqual() {
		return this.criteria.size() > 0
				&& Qualifier.FilterOperation.EQ.name().equals(this.criteria
						.keySet().toArray()[this.criteria.size() - 1]);
	}

	private boolean lastOperatorWasNotRange() {
		return this.criteria.size() > 0
				&& Qualifier.FilterOperation.BETWEEN.name().equals(this.criteria
						.keySet().toArray()[this.criteria.size() - 1]);
	}

	public Criteria nin(Object nextAsArray) {
		// TODO Auto-generated method stub
		return null;
	}

	public Criteria in(Object next) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Creates an 'or' criteria using the $or operator for all of the provided criteria
	 */
	public Criteria orOperator(Criteria... criteria) {
		this.key = Qualifier.FilterOperation.OR.name();
		return registerCriteriaChainElement(criteria);
	}
	
	private Criteria registerCriteriaChainElement(Criteria... criteria) {
		for(Criteria c : criteria){
			if (lastOperatorWasNot()) {
				throw new IllegalArgumentException(
						"operator $not is not allowed around criteria chain element: " + c.getCriteriaObject());
			} else {
				criteriaChain.add(c);
			}
		}
		return this;
	}

	/**
	 * @return the criteriaChain
	 */
	public List<Criteria> getCriteriaChain() {
		return criteriaChain;
	}

	public Criteria is(Object o, String propertyName) {
		if (!isValue.equals(NOT_SET)) {
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Multiple 'is' values declared. You need to use 'and' with multiple criteria");
		}

		if (lastOperatorWasNot()) {
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Invalid query: 'not' can't be used with 'is' - use 'ne' instead.");
		}

		if (lastOperatorWasNotRange()) {
			throw new InvalidAerospikeDataAccessApiUsageException(
					"Invalid query: cannot combine range with is");
		}

		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.EQ, Value.get(o));

		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.EQ.name(), qualifier);
		return this;
	}

	public Criteria between(Object o1, Object o2,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.BETWEEN, Value.get(o1),
				Value.get(o2));
		this.criteria.put(Qualifier.FilterOperation.BETWEEN.name(), qualifier);
		return this;
	}

	public Criteria startingWith(Object o,String propertyName, IgnoreCaseType ignoreCase) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.START_WITH, ignoreCase==IgnoreCaseType.ALWAYS, Value.get(o));
		this.criteria.put(Qualifier.FilterOperation.START_WITH.name(),
				qualifier);
		return this;
	}

	public Criteria containing(Object o,String propertyName, IgnoreCaseType ignoreCase) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.CONTAINING, ignoreCase==IgnoreCaseType.ALWAYS, Value.get(o));
		this.criteria.put(Qualifier.FilterOperation.CONTAINING.name(),
				qualifier);
		return this;
	}

	/**
	 * GEO Query with distance from a geo location given longitude/latitude
	 */
	public Criteria geo_within(Object lng, Object lat, Object radius, String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.GEO_WITHIN, Value.get(String.format("{ \"type\": \"AeroCircle\", "
						  + "\"coordinates\": [[%.8f, %.8f], %f] }",
						  lng, lat, radius)));
		this.criteria.put(Qualifier.FilterOperation.GEO_WITHIN.name(), qualifier);
		return this;
	}
}
