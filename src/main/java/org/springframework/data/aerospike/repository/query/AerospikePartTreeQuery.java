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

import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikePartTreeQuery extends BaseAerospikePartTreeQuery {
	
	private final AerospikeOperations aerospikeOperations;

	public AerospikePartTreeQuery(QueryMethod queryMethod,
								  QueryMethodEvaluationContextProvider evalContextProvider,
								  AerospikeOperations aerospikeOperations,
								  Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
		super(queryMethod, evalContextProvider, queryCreator);
		this.aerospikeOperations = aerospikeOperations;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Object execute(Object[] parameters) {
		ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
		Query query = prepareQuery(parameters, accessor);

		Class<?> targetClass = getTargetClass(accessor);

		if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
			Stream<?> result = findByQuery(query, targetClass);
			long total = queryMethod.isSliceQuery() ? 0 : aerospikeOperations.count(query, queryMethod.getEntityInformation().getJavaType());
			//TODO: should return SliceImpl for slice query
			return new PageImpl(result.collect(Collectors.toList()), accessor.getPageable(), total);
		} else if (queryMethod.isStreamQuery()) {
			return findByQuery(query, targetClass);
		} else if (queryMethod.isCollectionQuery()) {
			return findByQuery(query, targetClass).collect(Collectors.toList());
		} else if (queryMethod.isQueryForEntity()) {
			Stream<?> result = findByQuery(query, targetClass);
			return result.findFirst().orElse(null);
		}
		throw new UnsupportedOperationException("Query method " + queryMethod.getNamedQueryName() + " not supported.");
	}

	private Class<?> getTargetClass(ParametersParameterAccessor accessor) {
		// Dynamic projection
		if (accessor.getParameters().hasDynamicProjection()) {
			return accessor.findDynamicProjection();
		}
		// DTO projection
		if (queryMethod.getReturnedObjectType() != queryMethod.getEntityInformation().getJavaType()) {
			return queryMethod.getReturnedObjectType();
		}
		// No projection - target class will be the entity class.
		return queryMethod.getEntityInformation().getJavaType();
	}

	private Stream<?> findByQuery(Query query, Class<?> targetClass) {
		// Run query and map to different target class.
		if (targetClass != queryMethod.getEntityInformation().getJavaType()) {
			return aerospikeOperations.find(query, queryMethod.getEntityInformation().getJavaType(), targetClass);
		}
		// Run query and map to entity class type.
		return aerospikeOperations.find(query, queryMethod.getEntityInformation().getJavaType());
	}
}
