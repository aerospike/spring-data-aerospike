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
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.TemplateUtils.*;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;
import static org.springframework.data.aerospike.query.QualifierUtils.queryCriteriaIsNotNull;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikePartTreeQuery extends BaseAerospikePartTreeQuery {

    private final AerospikeOperations operations;

    public AerospikePartTreeQuery(QueryMethod queryMethod,
                                  QueryMethodValueEvaluationContextAccessor evalContextAccessor,
                                  AerospikeTemplate operations,
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
            return processPaginatedQuery(targetClass, accessor.getPageable(), query);
        } else if (queryMethod.isStreamQuery()) {
            return findByQuery(query, targetClass);
        } else if (queryMethod.isCollectionQuery()) {
            // All queries with Collection return type including projections
            return findByQuery(query, targetClass).collect(Collectors.toList());
        } else if (queryMethod.isQueryForEntity()) {
            Stream<?> result = findByQuery(query, targetClass);
            return result.findFirst().orElse(null);
        }
        throw new UnsupportedOperationException("Query method " + queryMethod.getNamedQueryName() + " is not " +
            "supported");
    }

    protected Object runQueryWithIdsEquality(Class<?> targetClass, List<Object> ids, Query query, Pageable pageable) {
        if (isExistsQuery(queryMethod)) {
            return operations.existsByIdsUsingQuery(ids, entityClass, query);
        } else if (isCountQuery(queryMethod)) {
            return operations.countExistingByIdsUsingQuery(ids, entityClass, query);
        } else if (isDeleteQuery(queryMethod)) {
            operations.deleteByIdsUsingQuery(ids, entityClass, query);
            return Optional.empty();
        } else {
            if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
                return processPaginatedIdQuery(targetClass, ids, pageable, query);
            }
            Stream<?> results = operations.findByIdsUsingQuery(ids, entityClass, targetClass, query)
                .filter(Objects::nonNull);
            return applyPostProcessing(results, query);
        }
    }

    private Object processPaginatedIdQuery(Class<?> targetClass, List<Object> ids, Pageable pageable, Query query) {
        if (!queryCriteriaIsNotNull(query) && !pageable.isUnpaged() && query.getSort() == null) {
            // Paginated queries with offset and no sorting (i.e. original order)
            // are only allowed for purely id queries, and not for other queries
            // Limit by page size + 1 to be able to initialize SliceImpl, apply post-processing beforehand
            List<Object> idsPagedPlusOne = ids.stream()
                .skip(pageable.getOffset()) // query offset
                .limit(pageable.getPageSize() + 1) // query rows
                .toList(); // no custom sorting in this case, which allows to prepare for reading just page size + 1

            Stream<?> pagedResultsPlusOne =
                operations.findByIdsUsingQuery(idsPagedPlusOne, entityClass, targetClass, query);
            return processPagedQueryWithIdsOnly(ids, pagedResultsPlusOne, pageable);
        }
        // Purely id queries with pageable.isUnpaged() get processed here
        Stream<?> unprocessedResultsStream =
            operations.findByIdsUsingQuery(ids, entityClass, targetClass, query);
            return processPagedQuery(unprocessedResultsStream, pageable, query);
    }

    private Object processPaginatedQuery(Class<?> targetClass, Pageable pageable, Query query) {
        Stream<?> unprocessedResultsStream =
            operations.findUsingQueryWithoutPostProcessing(entityClass, targetClass, query);
        return processPagedQuery(unprocessedResultsStream, pageable, query);
    }

    private Object processPagedQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        if (queryMethod.isSliceQuery()) {
            return processSliceQuery(unprocessedResultsStream, pageable, query);
        }
        return processPageQuery(unprocessedResultsStream, pageable, query);
    }

    private Object processPagedQueryWithIdsOnly(List<Object> ids, Stream<?> pagedResultsPlusOne, Pageable pageable) {
        if (queryMethod.isSliceQuery()) {
            return processSliceQueryWithIdsOnly(pagedResultsPlusOne, pageable);
        }
        return processPageQueryWithIdsOnly(ids, pagedResultsPlusOne.limit(pageable.getPageSize()), pageable);
    }

    /**
     * Creates new SliceImpl based on given parameters.
     * This method is used for paginated regular and combined queries, and for purely id queries where
     * pageable.isUnpaged() is true.
     * <br>
     * The case of paginated purely id queries is processed within {@link #processSliceQueryWithIdsOnly(Stream, Pageable)}
     */
    private Object processSliceQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        if (pageable.isUnpaged()) {
            return new SliceImpl<>(unprocessedResultsStream.toList(), pageable, false);
        }

        Query limitedQueryPlusOne = query.limit(pageable.getPageSize() + 1);
        List<Object> limitedResultsPlusOne = applyPostProcessing(unprocessedResultsStream, limitedQueryPlusOne)
            .collect(Collectors.toList());

        boolean hasNext = limitedResultsPlusOne.size() > pageable.getPageSize();
        if (hasNext) limitedResultsPlusOne = limitedResultsPlusOne.subList(0, pageable.getPageSize());
        return new SliceImpl<>(limitedResultsPlusOne, pageable, hasNext);
    }

    /**
     * Creates new SliceImpl based on given parameters.
     * This method is used for paginated purely id queries.
     * <br>
     * The case when pageable.isUnpaged() is true is processed within {@link #processSliceQuery(Stream, Pageable, Query)}
     */
    private Object processSliceQueryWithIdsOnly(Stream<?> pagedResultsPlusOne, Pageable pageable) {
        List<Object> results = pagedResultsPlusOne.collect(Collectors.toList());
        boolean hasNext = results.size() > pageable.getPageSize();
        if (hasNext) results = results.subList(0, pageable.getPageSize());
        return new SliceImpl<>(results, pageable, hasNext);
    }

    /**
     * Creates new PageImpl based on given parameters.
     * This method is used for paginated regular and combined queries, and for purely id queries where
     * pageable.isUnpaged() is true.
     * <br>
     * The case of paginated purely id queries is processed within {@link #processPageQueryWithIdsOnly(List, Stream, Pageable)}
     */
    private Object processPageQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        long numberOfAllResults;
        List<?> resultsPage;
            List<?> unprocessedResults = unprocessedResultsStream.toList();
            numberOfAllResults = unprocessedResults.size();
            resultsPage = pageable.isUnpaged() ? unprocessedResults : applyPostProcessing(unprocessedResults.stream(),
                query).toList();
        return new PageImpl<>(resultsPage, pageable, numberOfAllResults);
    }

    /**
     * Creates new PageImpl based on given parameters.
     * This method is used for paginated purely id queries.
     * <br>
     * The case when pageable.isUnpaged() is true is processed within {@link #processPageQuery(Stream, Pageable, Query)}
     */
    private Object processPageQueryWithIdsOnly(List<Object> ids, Stream<?> pagedResults, Pageable pageable) {
        List<?> resultsPage = pagedResults.toList();
        return new PageImpl<>(resultsPage, pageable, ids.size());
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
