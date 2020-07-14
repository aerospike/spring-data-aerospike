/*
 * Copyright 2012-2017 Aerospike, Inc.
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
package org.springframework.data.aerospike.repository.query;

import com.aerospike.client.Value;
import org.springframework.data.aerospike.query.Qualifier;

/**
 * @author Michael Zhang
 * @author Jeff Boone
 * 
 */
public class AerospikeCriteria extends Qualifier implements CriteriaDefinition {
	
	public AerospikeCriteria(FilterOperation operation, Qualifier... qualifiers) {
		super(operation, qualifiers);
	}
	
	public AerospikeCriteria(String field, FilterOperation operation, Boolean ignoreCase, Value value1) {
		super(field, operation, ignoreCase, value1);
	}

	public AerospikeCriteria(String field, FilterOperation operation, Value value1, Value value2) {
		super(field, operation, value1, value2);		
	}

	/**
	 * Creates an 'or' criteria using the $or operator for all of the provided criteria
	 *
	 * @throws IllegalArgumentException if {@link #orOperator(Criteria...)} follows a not() call directly.
	 * @param criteria
	 */
	public static AerospikeCriteria or(AerospikeCriteria... criteria) {
		return new AerospikeCriteria(Qualifier.FilterOperation.OR, criteria);
	}
	


	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.CriteriaDefinition#getCriteriaObject()
	 */
	@Override
	public Qualifier getCriteriaObject() {
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.CriteriaDefinition#getKey()
	 */
	@Override
	public String getKey() {
		return this.getField();
	}

}
