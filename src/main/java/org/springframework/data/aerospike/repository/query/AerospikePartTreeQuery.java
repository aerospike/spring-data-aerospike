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
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

import java.util.Arrays;
import java.util.List;
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
    @SuppressWarnings({"unchecked", "rawtypes", "NullableProblems"})
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Query query = prepareQuery(parameters, accessor);

        Class<?> targetClass = getTargetClass(accessor);

        // "findById" has its own processing flow
        if (isIdProjectionQuery(targetClass, parameters, query.getAerospikeCriteria())) {
            Object accessorValue = accessor.getBindableValue(0);
            Object result;
            if (accessorValue == null) {
                throw new IllegalStateException("Parameters accessor value is null while parameters quantity is > 0");
            } else if (accessorValue.getClass().isArray()) {
                result = aerospikeOperations.findByIds(Arrays.stream(((Object[]) accessorValue)).toList(),
                    sourceClass, targetClass);
            } else if (accessorValue instanceof Iterable<?>) {
                result = aerospikeOperations.findByIds((Iterable<?>) accessorValue, sourceClass, targetClass);
            } else {
                result = aerospikeOperations.findById(accessorValue, sourceClass, targetClass);
            }
            return result;
        }

        if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
            Stream<?> result = findByQuery(query, targetClass);
            List<?> results = result.toList();
            Pageable pageable = accessor.getPageable();
            long numberOfAllResults = aerospikeOperations.count(query, sourceClass);

            if (queryMethod.isSliceQuery()) {
                boolean hasNext = numberOfAllResults > pageable.getPageSize() * (pageable.getOffset() + 1);
                return new SliceImpl(results, pageable, hasNext);
            } else {
                return new PageImpl(results, pageable, numberOfAllResults);
            }
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

    private Stream<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class.
        if (targetClass != sourceClass) {
            return aerospikeOperations.find(query, queryMethod.getEntityInformation().getJavaType(), targetClass);
        }
        // Run query and map to entity class type.
        return aerospikeOperations.find(query, sourceClass);
    }
}
