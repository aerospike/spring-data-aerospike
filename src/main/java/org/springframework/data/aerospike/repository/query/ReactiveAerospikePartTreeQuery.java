/*
 * Copyright 2012-2019 the original author or authors
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

import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.springframework.data.aerospike.core.TemplateUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.core.TemplateUtils.getIdValue;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikePartTreeQuery extends BaseAerospikePartTreeQuery {

    private final ReactiveAerospikeTemplate template;

    public ReactiveAerospikePartTreeQuery(QueryMethod queryMethod,
                                          QueryMethodEvaluationContextProvider evalContextProvider,
                                          ReactiveAerospikeTemplate aerospikeTemplate,
                                          Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(queryMethod, evalContextProvider, queryCreator);
        this.template = aerospikeTemplate;
    }

    @Override
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Query query = prepareQuery(parameters, accessor);
        Class<?> targetClass = getTargetClass(accessor);

        // queries that include id have their own processing flow
        if (parameters != null && parameters.length > 0) {
            Qualifier criteria = query.getQualifier();
            List<Object> ids;
            if (criteria.hasSingleId()) {
                ids = getIdValue(criteria);
                return template.findByIdsUsingQuery(ids, entityClass, targetClass, null);
            } else {
                Qualifier idQualifier;
                if ((idQualifier = getIdQualifier(criteria)) != null) {
                    ids = getIdValue(idQualifier);
                    return template.findByIdsUsingQuery(ids, entityClass, targetClass,
                        new Query(excludeIdQualifier(criteria)));
                }
            }
        }

        if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
            Flux<?> results = template.findUsingQueryWithoutPostProcessing(entityClass, targetClass, query);
            Mono<? extends List<?>> unprocessedResultsListMono = results.collectList();
            Mono<Long> sizeMono = results.count();
            return sizeMono.flatMap(size ->
                unprocessedResultsListMono.map(list -> getPaginatedResult(list, size.intValue(), accessor, query))
            );
        }

        return findByQuery(query, targetClass);
    }

    public Object getPaginatedResult(List<?> unprocessedResults, int overallSize, ParametersParameterAccessor accessor,
                                  Query query) {
        List<?> resultsPaginated = applyPostProcessingOnResults(unprocessedResults.stream(), query).toList();
        Pageable pageable = accessor.getPageable();
        if (queryMethod.isSliceQuery()) {
            boolean hasNext = overallSize > pageable.getPageSize() * (pageable.getOffset() + 1);
            return new SliceImpl<>(resultsPaginated, pageable, hasNext);
        } else {
            return new PageImpl<>(resultsPaginated, pageable, overallSize);
        }
    }

    private Flux<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class.
        if (targetClass != entityClass) {
            return template.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type.
        return template.find(query, entityClass);
    }
}
