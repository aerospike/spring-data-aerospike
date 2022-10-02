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

import java.util.HashMap;
import java.util.Map;

/**
 * @author Michael Zhang
 * @author Jeff Boone
 */
public class AerospikeCriteria extends Qualifier implements CriteriaDefinition {
	
	public AerospikeCriteria(AerospikeCriteriaBuilder builder) {
		super(builder);
	}

	/**
	 * Creates an 'or' criteria using the $or operator for all of the provided criteria.
	 *
	 * @param criteria the AerospikeCriteria instance.
	 * @throws IllegalArgumentException if follows a not() call directly.
	 */
	public static AerospikeCriteria or(AerospikeCriteria... criteria) {
		return new AerospikeCriteria(new AerospikeCriteriaBuilder()
				.setFilterOperation(Qualifier.FilterOperation.OR)
				.setQualifiers(criteria));
	}

	@Override
	public Qualifier getCriteriaObject() {
		return this;
	}

	@Override
	public String getKey() {
		return this.getField();
	}

	public static class AerospikeCriteriaBuilder {
		private final Map<String, Object> map = new HashMap<>();

		public AerospikeCriteriaBuilder() {
		}

		public AerospikeCriteriaBuilder setField(String field) {
			this.map.put(FIELD, field);
			return this;
		}

		public AerospikeCriteriaBuilder setIgnoreCase(boolean ignoreCase) {
			this.map.put(IGNORE_CASE, ignoreCase);
			return this;
		}

		public AerospikeCriteriaBuilder setFilterOperation(FilterOperation filterOperation) {
			this.map.put(OPERATION, filterOperation);
			return this;
		}

		public AerospikeCriteriaBuilder setQualifiers(Qualifier... qualifiers) {
			this.map.put(QUALIFIERS, qualifiers);
			return this;
		}

		public AerospikeCriteriaBuilder setValue1(Value value1) {
			this.map.put(VALUE1, value1);
			return this;
		}

		public AerospikeCriteriaBuilder setValue2(Value value2) {
			this.map.put(VALUE2, value2);
			return this;
		}

		public AerospikeCriteriaBuilder setValue3(Value value3) {
			this.map.put(VALUE3, value3);
			return this;
		}

		public AerospikeCriteria build() {
			return new AerospikeCriteria(this);
		}

		public Map<String, Object> buildMap() {
			return this.map;
		}
	}
}
