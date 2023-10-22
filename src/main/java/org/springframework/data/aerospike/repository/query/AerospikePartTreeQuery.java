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

import org.springframework.data.aerospike.core.AerospikeInternalOperations;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.TemplateUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.core.TemplateUtils.getIdValue;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;
import static org.springframework.data.aerospike.query.QualifierUtils.getQualifiers;
import static org.springframework.data.aerospike.repository.query.AerospikeCriteria.isSingleIdQuery;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikePartTreeQuery extends BaseAerospikePartTreeQuery {

    private final AerospikeOperations operations;
    private final AerospikeInternalOperations internalOperations;

    public AerospikePartTreeQuery(QueryMethod queryMethod,
                                  QueryMethodEvaluationContextProvider evalContextProvider,
                                  AerospikeTemplate aerospikeTemplate,
                                  Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(queryMethod, evalContextProvider, queryCreator);

        this.operations = aerospikeTemplate;
        this.internalOperations = aerospikeTemplate;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes", "NullableProblems"})
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Query query = prepareQuery(parameters, accessor);
        Class<?> targetClass = getTargetClass(accessor);

        // queries that include id have their own processing flow
        if (parameters != null && parameters.length > 0) {
            AerospikeCriteria criteria = query.getAerospikeCriteria();
            Qualifier[] qualifiers = getQualifiers(criteria);
            Qualifier idQualifier;
            if (isSingleIdQuery(criteria)) {
                return runIdQuery(entityClass, targetClass, getIdValue(qualifiers[0]));
            } else {
                if ((idQualifier = getIdQualifier(criteria)) != null) {
                    return runIdQuery(entityClass, targetClass, getIdValue(idQualifier),
                        excludeIdQualifier(qualifiers));
                }
            }
        }

        if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
            Stream<?> result = findByQuery(query, targetClass);
            List<?> results = result.toList();
            Pageable pageable = accessor.getPageable();
            long numberOfAllResults = operations.count(query, entityClass);

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
        throw new UnsupportedOperationException("Query method " + queryMethod.getNamedQueryName() + " is not " +
            "supported");
    }

    protected Object findByIds(Collection<?> ids, Class<?> entityClass, Class<?> targetClass,
                               Qualifier... qualifiers) {
        return internalOperations.findByIdsInternal(ids, entityClass, targetClass, qualifiers);
    }

    private Stream<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class.
        if (targetClass != null && targetClass != entityClass) {
            return operations.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type.
        return operations.find(query, entityClass);
    }
}
