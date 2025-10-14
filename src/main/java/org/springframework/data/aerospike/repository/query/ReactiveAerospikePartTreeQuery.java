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
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.springframework.data.aerospike.core.PostProcessingUtils.applyPostProcessingOnResults;
import static org.springframework.data.aerospike.core.QualifierUtils.getIdValue;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikePartTreeQuery extends BaseAerospikePartTreeQuery<Flux<?>> {

    private final ReactiveAerospikeTemplate template;
    private final AerospikeQueryMethod queryMethod;
    private final String namespace;
    private final Map<IndexKey, Index> indexCache;

    public ReactiveAerospikePartTreeQuery(QueryMethod queryMethod,
                                          QueryMethodValueEvaluationContextAccessor evalContextAccessor,
                                          ReactiveAerospikeTemplate template,
                                          Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(queryMethod, evalContextAccessor, queryCreator, (AerospikeMappingContext) template.getMappingContext(),
            template.getAerospikeConverter(), template.getServerVersionSupport(), template.getDSLParser());
        this.template = template;
        this.namespace = template.getNamespace();
        this.indexCache = template.getIndexesCache();
        // each queryMethod here is AerospikeQueryMethod
        this.queryMethod = (AerospikeQueryMethod) queryMethod;
    }

    @Override
    @SuppressWarnings({"NullableProblems"})
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Class<?> targetClass = getTargetClass(accessor, queryMethod);

        if (queryMethod.hasQueryAnnotation()) {
            return findByQueryAnnotation(queryMethod, targetClass, namespace, indexCache, parameters);
        }
        Query query = prepareQuery(parameters, accessor);

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
            Pageable pageable = accessor.getPageable();
            Flux<?> unprocessedResults = template.findUsingQueryWithoutPostProcessing(entityClass, targetClass,
                query);
            return processPagedQuery(unprocessedResults, pageable, query);
        } else if (queryMethod.isStreamQuery()) {
            return findByQuery(query, targetClass).toStream();
        } else if (queryMethod.isCollectionQuery()) {
            // Currently there seems to be no way to distinguish return type Collection from Mono<Collection> etc.,
            // so a query method with return type Collection will compile but throw ClassCastException in runtime
            return findByQuery(query, targetClass).collectList();
        } else if (queryMethod.isQueryForEntity() || !isEntityAssignableFromReturnType(queryMethod)) {
            // Queries with Flux<Entity> and Mono<Entity> return types including projection queries
            return findByQuery(query, targetClass);
        }
        throw new UnsupportedOperationException("Query method " + queryMethod.getNamedQueryName() + " is not " +
            "supported");
    }

    /**
     * Runs {@link ReactiveAerospikeTemplate#find(Query, Class)} for given query, results are mapped
     * to the original entityClass or to the given {@code targetClass}, then post-processing is applied on the results.
     */
    @Override
    protected Flux<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class
        if (targetClass != entityClass) {
            return template.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type
        return template.find(query, entityClass);
    }

    /**
     * Runs ids-based query when ids are compared for equality.
     * Depending on query method, runs either exists, count, delete or find query.
     */
    protected Object runQueryWithIdsEquality(Class<?> targetClass, List<Object> ids, Query query, Pageable pageable) {
        if (isExistsQuery(queryMethod)) {
            return template.existsByIdsUsingQuery(ids, entityClass, query);
        } else if (isCountQuery(queryMethod)) {
            return template.countByIdsUsingQuery(ids, entityClass, query);
        } else if (isDeleteQuery(queryMethod)) {
            return template.deleteByIdsUsingQuery(ids, entityClass, query);
        } else {
            if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
                return processPaginatedIdQuery(targetClass, ids, pageable, query);
            }
            return template.findByIdsUsingQuery(ids, entityClass, targetClass, query);
        }
    }

    /**
     * Processes ids-based paginated query: performs batch read and applies post-processing.
     * @return {@link Mono<Page>} or {@link Mono<Slice>}
     */
    private Mono<?> processPaginatedIdQuery(Class<?> targetClass, List<Object> ids, Pageable pageable, Query query) {
        // Combined queries with ids
        Flux<?> unprocessedResultsStream =
            template.findByIdsWithoutPostProcessing(ids, entityClass, targetClass, query)
                .filter(Objects::nonNull); // Leave only existing records
        // Post-processing is done separately here
        return processPagedQuery(unprocessedResultsStream, pageable, query);
    }

    /**
     * Applies post-processing on results of batch read for paginated queries.
     *
     * @return {@link Mono<Page>} or {@link Mono<Slice>}
     */
    private Mono<?> processPagedQuery(Flux<?> unprocessedResults, Pageable pageable, Query query) {
        if (queryMethod.isSliceQuery()) {
            return processSliceQuery(unprocessedResults, pageable, query);
        }
        return processPageQuery(unprocessedResults, pageable, query);
    }

    /**
     * Creates new SliceImpl based on given parameters. This method is used for paginated queries.
     */
    private Mono<Slice<?>> processSliceQuery(Flux<?> unprocessedResults, Pageable pageable, Query query) {
        return unprocessedResults
            .collectList()
            .map(list -> {
                if (list.isEmpty() || pageable.isUnpaged()) {
                    return new SliceImpl<>(list, pageable, false);
                }

                // Override query's limit (rows) before applying post-processing to return +1 indicating hasNext
                // Query's offset and sorting are set from the pageable
                Query limitedQueryPlusOne = query.limit(pageable.getPageSize() + 1);
                List<Object> limitedResultsPlusOne = applyPostProcessingOnResults(list.stream(), limitedQueryPlusOne)
                    .collect(Collectors.toList());

                boolean hasNext = limitedResultsPlusOne.size() > pageable.getPageSize();
                if (hasNext) limitedResultsPlusOne = limitedResultsPlusOne.subList(0, pageable.getPageSize());
                return new SliceImpl<>(limitedResultsPlusOne, pageable, hasNext);
            });
    }

    /**
     * Creates new PageImpl based on given parameters. This method is used for paginated queries.
     */
    private Mono<Page<?>> processPageQuery(Flux<?> unprocessedResults, Pageable pageable, Query query) {
        return unprocessedResults
            .collectList()
            .map(list -> {
                List<?> resultsPage;
                if (list.isEmpty() || pageable.isUnpaged()) {
                    resultsPage = list;
                } else {
                    resultsPage = applyPostProcessingOnResults(list.stream(), query).toList();
                }
                return new PageImpl<>(resultsPage, pageable, list.size());
            });
    }
}
