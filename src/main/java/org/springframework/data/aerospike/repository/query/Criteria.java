/**
 * 
 */
package org.springframework.data.aerospike.repository.query;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.aerospike.InvalidAerospikeDataAccessApiUsageException;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.util.CollectionUtils;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.Qualifier.FilterOperation;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
public class Criteria implements CriteriaDefinition {

	private static final Object NOT_SET = new Object();

	DefaultConversionService cs = new DefaultConversionService();

	private String key;
	private List<Criteria> criteriaChain;
	private LinkedHashMap<String, Object> criteria = new LinkedHashMap<String, Object>();
	private Object isValue = NOT_SET;

	public Criteria(String key) {
		this.criteriaChain = new ArrayList<Criteria>();
		this.criteriaChain.add(this);
		this.key = key;
	}

	protected Criteria(List<Criteria> criteriaChain, String key) {
		this.criteriaChain = criteriaChain;
		this.criteriaChain.add(this);
		this.key = key;
	}

	/**
	 * 
	 */
	public Criteria() {
		this.criteriaChain = new ArrayList<Criteria>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.aerospike.repository.query.CriteriaDefinition#
	 * getCriteriaObject()
	 */
	@Override
	public Qualifier getCriteriaObject() {
		if (this.criteriaChain.size() == 1) {
			return criteriaChain.get(0).getSingleCriteriaObject();
		} else if (CollectionUtils.isEmpty(this.criteriaChain) && !CollectionUtils.isEmpty(this.criteria)){
			return getSingleCriteriaObject();
		} else {
			FilterOperation op = FilterOperation.valueOf(key);
			List<Qualifier> qualifiers = new ArrayList<Qualifier>();
			for (Criteria c : this.criteriaChain) {
				qualifiers.add(c.getCriteriaObject());
			}
			return new Qualifier(op, (Qualifier[]) qualifiers.toArray(new Qualifier[qualifiers.size()]));
		}
	}

	protected Qualifier getSingleCriteriaObject() {
		Qualifier qualifier = null;
		for (String k : this.criteria.keySet()) {
			qualifier = (Qualifier) this.criteria.get(k);
		}

		return qualifier;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.aerospike.repository.query.CriteriaDefinition#
	 * getKey()
	 */
	@Override
	public String getKey() {
		return this.key;
	}

	/**
	 * Static factory method to create a Criteria using the provided key
	 * 
	 * @param key
	 * @return
	 */
	public static Criteria where(String key) {
		return new Criteria(key);
	}

	/**
	 * Static factory method to create a Criteria using the provided key
	 * 
	 * @return
	 */
	public Criteria and(String key) {
		return new Criteria(this.criteriaChain, key);
	}
	
	/**
	 * @param property
	 * @param next
	 * @return
	 */
	public Criteria gt(Object o, String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.GT, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.GT.name(), qualifier);
		return this;
	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria gte(Object o,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.GTEQ, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.GTEQ.name(), qualifier);
		return this;

	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria lt(Object o,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.LT, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.LT.name(), qualifier);
		return this;
	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria lte(Object o,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.LTEQ, Value.get(o));
		this.isValue = o;
		this.criteria.put(Qualifier.FilterOperation.LTEQ.name(), qualifier);
		return this;
	}

	/**
	 * @param object
	 * @return
	 */
	public Criteria ne(Object object) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @return
	 */
	private boolean lastOperatorWasNot() {
		return this.criteria.size() > 0 && "$not".equals(
				this.criteria.keySet().toArray()[this.criteria.size() - 1]);
	}

	/**
	 * @return
	 */
	@SuppressWarnings("unused")
	private boolean lastOperatorWasNotEqual() {
		return this.criteria.size() > 0
				&& Qualifier.FilterOperation.EQ.name().equals(this.criteria
						.keySet().toArray()[this.criteria.size() - 1]);
	}

	/**
	 * @return
	 */
	private boolean lastOperatorWasNotRange() {
		return this.criteria.size() > 0
				&& Qualifier.FilterOperation.BETWEEN.name().equals(this.criteria
						.keySet().toArray()[this.criteria.size() - 1]);
	}

	/**
	 * @param nextAsArray
	 * @return
	 */
	public Criteria nin(Object nextAsArray) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @param next
	 * @return
	 */
	public Criteria in(Object next) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Creates an 'or' criteria using the $or operator for all of the provided criteria
	 *
	 * @throws IllegalArgumentException if {@link #orOperator(Criteria...)} follows a not() call directly.
	 * @param criteria
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

	/**
	 * @param next
	 * @param part
	 * @param criteria2
	 * @return
	 */
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

	/**
	 * @param next
	 * @param next2
	 * @param part
	 * @param criteria2
	 * @return
	 */
	public Criteria between(Object o1, Object o2,String propertyName) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.BETWEEN, Value.get(o1),
				Value.get(o2));
		this.criteria.put(Qualifier.FilterOperation.BETWEEN.name(), qualifier);
		return this;

	}

	/**
	 * @param next
	 * @param part
	 * @param criteria2
	 * @return
	 */
	public Criteria startingWith(Object o,String propertyName, IgnoreCaseType ignoreCase) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.START_WITH, ignoreCase==IgnoreCaseType.ALWAYS, Value.get(o));
		this.criteria.put(Qualifier.FilterOperation.START_WITH.name(),
				qualifier);
		return this;

	}

	/**
	 * @param next
	 * @param part
	 * @param criteria2
	 * @return
	 */
	public Criteria containing(Object o,String propertyName, IgnoreCaseType ignoreCase) {
		Qualifier qualifier = new Qualifier(propertyName,
				Qualifier.FilterOperation.CONTAINING, ignoreCase==IgnoreCaseType.ALWAYS, Value.get(o));
		this.criteria.put(Qualifier.FilterOperation.CONTAINING.name(),
				qualifier);
		return this;

	}

	/***
	 * GEO Query with distance from a geo location given longitude/latitude 
	 * @param lng
	 * @param lat
	 * @param radius
	 * @param propertyName
	 * @return
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