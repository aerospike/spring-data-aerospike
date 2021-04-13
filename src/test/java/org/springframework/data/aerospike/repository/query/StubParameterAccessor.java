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

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class StubParameterAccessor implements AerospikeParameterAccessor {
	
	private final Object[] values;

	public StubParameterAccessor(Object... values) {
		this.values = values;
	}
	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getPageable()
	 */
	@Override
	public Pageable getPageable() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getSort()
	 */
	@Override
	public Sort getSort() {
		return null;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#getBindableValue(int)
	 */
	@Override
	public Object getBindableValue(int index) {
		return values[index];
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#hasBindableNullValue()
	 */
	@Override
	public boolean hasBindableNullValue() {
		return false;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParameterAccessor#iterator()
	 */
	@Override
	public Iterator<Object> iterator() {
		return Arrays.asList(values).iterator();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.aerospike.repository.query.AerospikeParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {
		return this.values;
	}

	/**
	 * @deprecated
	 */
	@Override
	public Optional<Class<?>> getDynamicProjection() {
		return Optional.empty();
	}

	@Override
	public Class<?> findDynamicProjection() {
		return null;
	}
}
