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

import org.springframework.data.aerospike.core.ReactiveAerospikeOperations;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.TemplateUtils.*;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikePartTreeQuery extends BaseAerospikePartTreeQuery {

    private final ReactiveAerospikeOperations operations;

    public ReactiveAerospikePartTreeQuery(QueryMethod queryMethod,
                                          QueryMethodValueEvaluationContextAccessor evalContextAccessor,
                                          ReactiveAerospikeTemplate operations,
                                          Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(queryMethod, evalContextAccessor, queryCreator, (AerospikeMappingContext) operations.getMappingContext(),
            operations.getAerospikeConverter(), operations.getServerVersionSupport());
        this.operations = operations;
    }

    @Override
    @SuppressWarnings({"NullableProblems"})
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Query query = prepareQuery(parameters, accessor);
        Class<?> targetClass = getTargetClass(accessor);

        // queries with id equality have their own processing flow
        if (parameters != null && parameters.length > 0) {
            Qualifier criteria = query.getCriteriaObject();
            // only for id EQ, id LIKE queries have SimpleProperty query creator
            if (criteria.hasSingleId()) {
                // Read id only
                // It cannot have sorting, offset, rows limited or be distinct, thus query is not transferred
                return runQueryWithIdsEquality(targetClass, getIdValue(criteria), null, accessor.getPageable());
            } else {
                // Combined query with ids
                Qualifier idQualifier;
                if ((idQualifier = getIdQualifier(criteria)) != null) {
                    return runQueryWithIdsEquality(targetClass, getIdValue(idQualifier),
                        getQueryWithExcludedIdQualifier(query, criteria), accessor.getPageable());
                }
            }
        }

        if (isExistsQuery(queryMethod)) {
            return operations.exists(query, queryMethod.getEntityInformation().getJavaType());
        } else if (isCountQuery(queryMethod)) {
            return operations.count(query, queryMethod.getEntityInformation().getJavaType());
        } else if (isDeleteQuery(queryMethod)) {
            operations.delete(query, queryMethod.getEntityInformation().getJavaType());
            return Optional.empty();
        } else if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
            Pageable pageable = accessor.getPageable();
            Flux<?> unprocessedResults = operations.findUsingQueryWithoutPostProcessing(entityClass, targetClass,
                query);
            Mono<Long> sizeMono = unprocessedResults.count();

            if (operations.getQueryMaxRecords() > 0) {
                Mono<? extends List<?>> unprocessedResultsListMono = unprocessedResults.collectList();
                return sizeMono.flatMap(size ->
                    unprocessedResultsListMono.map(list -> getPage(list, size, pageable, query))
                );
            }
            return sizeMono.map(size -> {
                if (pageable.isUnpaged()) {
                    Mono<? extends List<?>> unprocessedResultsListMono = unprocessedResults.collectList();
                    return unprocessedResultsListMono.map(list -> getPage(list, size, pageable, query));
                }
                return getPage(unprocessedResults, size, pageable, query);
            });
        } else if (queryMethod.isStreamQuery()) {
            return findByQuery(query, targetClass).toStream();
        } else if (queryMethod.isCollectionQuery()) {
            // Currently there seems to be no way to distinguish return type Collection from Mono<Collection> etc.,
            // so a query method with return type Collection will compile but throw ClassCastException in runtime
            return findByQuery(query, targetClass).collectList();
        }
         else if (queryMethod.isQueryForEntity() || !isEntityAssignableFromReturnType(queryMethod)) {
            // Queries with Flux<Entity> and Mono<Entity> return types including projection queries
            return findByQuery(query, targetClass);
        }
        throw new UnsupportedOperationException("Query method " + queryMethod.getNamedQueryName() + " is not " +
            "supported");
    }

    protected Object runQueryWithIdsEquality(Class<?> targetClass, List<Object> ids, Query query, Pageable isPagedQuery) {
        if (isExistsQuery(queryMethod)) {
            return operations.existsByIdsUsingQuery(ids, entityClass, query);
        } else if (isCountQuery(queryMethod)) {
            return operations.countByIdsUsingQuery(ids, entityClass, query);
        } else if (isDeleteQuery(queryMethod)) {
            return operations.deleteByIdsUsingQuery(ids, entityClass, query);
        } else {
            return operations.findByIdsUsingQuery(ids, entityClass, targetClass, query);
        }
    }

    public Object getPage(List<?> unprocessedResults, long overallSize, Pageable pageable, Query query) {
        if (queryMethod.isSliceQuery()) {
            return processSliceQuery(unprocessedResults, overallSize, pageable, query);
        } else {
            return processPageQuery(unprocessedResults, overallSize, pageable, query);
        }
    }

    public Object getPage(Flux<?> unprocessedResults, long overallSize, Pageable pageable, Query query) {
        if (queryMethod.isSliceQuery()) {
            List<?> resultsPaginated = applyPostProcessing(unprocessedResults, query).toList();
            boolean hasNext = overallSize > pageable.getPageSize() * (pageable.getOffset() + 1);
            return new SliceImpl<>(resultsPaginated, pageable, hasNext);
        }
        List<?> resultsPaginated = applyPostProcessing(unprocessedResults, query).toList();
        return new PageImpl<>(resultsPaginated, pageable, overallSize);
    }

    private Object processSliceQuery(List<?> unprocessedResults, long overallSize, Pageable pageable, Query query) {
        if (pageable.isUnpaged()) {
            return new SliceImpl<>(unprocessedResults, pageable, false);
        }
        List<?> resultsPaginated = applyPostProcessing(unprocessedResults.stream(), query).toList();
        boolean hasNext = overallSize > pageable.getPageSize() * (pageable.getOffset() + 1);
        return new SliceImpl<>(resultsPaginated, pageable, hasNext);
    }

    private Object processPageQuery(List<?> unprocessedResults, long overallSize, Pageable pageable, Query query) {
        if (pageable.isUnpaged()) {
            return new PageImpl<>(unprocessedResults, pageable, overallSize);
        }
        List<?> resultsPaginated = applyPostProcessing(unprocessedResults.stream(), query).toList();
        return new PageImpl<>(resultsPaginated, pageable, overallSize);
    }

    protected <T> Stream<T> applyPostProcessing(Flux<T> results, Query query) {
        if (query.getSort() != null && query.getSort().isSorted()) {
            Comparator<T> comparator = getComparator(query);
            results = results.sort(comparator);
        }
        if (query.hasOffset()) {
            results = results.skip(query.getOffset());
        }
        if (query.hasRows()) {
            results = results.take(query.getRows());
        }

        return results.toStream();
    }

    private Flux<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class.
        if (targetClass != entityClass) {
            return operations.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type.
        return operations.find(query, entityClass);
    }
}
