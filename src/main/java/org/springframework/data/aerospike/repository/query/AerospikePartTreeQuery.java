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

import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.lang.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.PostProcessingUtils.applyPostProcessingOnResults;
import static org.springframework.data.aerospike.core.QualifierUtils.getIdValue;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikePartTreeQuery extends BaseAerospikePartTreeQuery {

    private final AerospikeTemplate template;

    public AerospikePartTreeQuery(QueryMethod queryMethod,
                                  QueryMethodValueEvaluationContextAccessor evalContextAccessor,
                                  AerospikeTemplate template,
                                  Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(queryMethod, evalContextAccessor, queryCreator, (AerospikeMappingContext) template.getMappingContext(),
            template.getAerospikeConverter(), template.getServerVersionSupport());
        this.template = template;
    }

    @Override
    @SuppressWarnings({"NullableProblems"})
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Query query = prepareQuery(parameters, accessor);
        Class<?> targetClass = getTargetClass(accessor, queryMethod);

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
            return template.exists(query, queryMethod.getEntityInformation().getJavaType());
        } else if (isCountQuery(queryMethod)) {
            return template.count(query, queryMethod.getEntityInformation().getJavaType());
        } else if (isDeleteQuery(queryMethod)) {
            template.delete(query, queryMethod.getEntityInformation().getJavaType());
            return Optional.empty();
        } else if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
            Stream<?> unprocessedResultsStream =
                template.findUsingQueryWithoutPostProcessing(entityClass, targetClass, query);
            return processPagedQuery(unprocessedResultsStream, accessor.getPageable(), query);
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

    /**
     * Runs {@link AerospikeTemplate#find(Query, Class)} for given query, results are mapped
     * to the original entityClass or to the given {@code targetClass}, then post-processing is applied on the results.
     */
    private Stream<?> findByQuery(Query query, @Nullable Class<?> targetClass) {
        // Run query and map to different target class
        if (targetClass != null && targetClass != entityClass) {
            return template.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type
        return template.find(query, entityClass);
    }

    /**
     * Runs ids-based query when ids are compared for equality.
     * Depending on query method, runs either exists, count, delete or find query.
     */
    protected Object runQueryWithIdsEquality(Class<?> targetClass, List<Object> ids, @Nullable Query query,
                                             Pageable pageable) {
        if (isExistsQuery(queryMethod)) {
            return template.existsByIdsUsingQuery(ids, entityClass, query);
        } else if (isCountQuery(queryMethod)) {
            return template.countByIdsUsingQuery(ids, entityClass, query);
        } else if (isDeleteQuery(queryMethod)) {
            template.deleteByIdsUsingQuery(ids, entityClass, query);
            return Optional.empty();
        } else {
            if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
                return processPaginatedIdQuery(targetClass, ids, pageable, query);
            }
            return template.findByIdsUsingQuery(ids, entityClass, targetClass, query)
                .filter(Objects::nonNull);
        }
    }

    /**
     * Processes ids-based paginated query: performs batch read and applies filtering and post-processing.
     * @return {@link Page} or {@link Slice}
     */
    private Slice<?> processPaginatedIdQuery(Class<?> targetClass, List<Object> ids, Pageable pageable,
                                           @Nullable Query query) {
        // Combined queries with ids
        Stream<?> unprocessedResultsStream =
            template.findByIdsWithoutPostProcessing(ids, entityClass, targetClass, query)
                .filter(Objects::nonNull); // Leave only existing records
        // Post-processing is done separately here
        return processPagedQuery(unprocessedResultsStream, pageable, query);
    }

    /**
     * Applies post-processing on results of batch read for paginated queries.
     * @return {@link Page} or {@link Slice}
     */
    private Slice<?> processPagedQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        if (queryMethod.isSliceQuery()) {
            return processSliceQuery(unprocessedResultsStream, pageable, query);
        }
        return processPageQuery(unprocessedResultsStream, pageable, query);
    }

    /**
     * Creates new SliceImpl based on given parameters. This method is used for queries with
     * pagination.
     */
    private Slice<?> processSliceQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        if (pageable.isUnpaged()) {
            return new SliceImpl<>(unprocessedResultsStream.toList(), pageable, false);
        }

        // Override query's limit (rows) before applying post-processing to return +1 indicating hasNext
        // Query's offset and sorting are set from the pageable
        Query limitedQueryPlusOne = query.limit(pageable.getPageSize() + 1);
        List<Object> limitedResultsPlusOne = applyPostProcessingOnResults(unprocessedResultsStream, limitedQueryPlusOne)
            .collect(Collectors.toList());

        boolean hasNext = limitedResultsPlusOne.size() > pageable.getPageSize();
        if (hasNext) limitedResultsPlusOne = limitedResultsPlusOne.subList(0, pageable.getPageSize());
        return new SliceImpl<>(limitedResultsPlusOne, pageable, hasNext);
    }

    /**
     * Creates new PageImpl based on given parameters. This method is used for queries with pagination.
     */
    private Page<?> processPageQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        List<?> unprocessedResults = unprocessedResultsStream.toList();
        List<?> resultsPage;
        if (pageable.isUnpaged()) {
            resultsPage = unprocessedResults;
        } else {
            resultsPage = applyPostProcessingOnResults(unprocessedResults.stream(), query).toList();
        }
        return new PageImpl<>(resultsPage, pageable, unprocessedResults.size());
    }
}
