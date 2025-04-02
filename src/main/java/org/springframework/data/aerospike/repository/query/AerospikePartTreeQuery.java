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
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.TemplateUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.core.TemplateUtils.getIdValue;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikePartTreeQuery extends BaseAerospikePartTreeQuery<Stream<?>> {

    private final AerospikeOperations operations;
    private final AerospikeQueryMethod queryMethod;

    public AerospikePartTreeQuery(QueryMethod baseQueryMethod,
                                  QueryMethodValueEvaluationContextAccessor evalContextAccessor,
                                  AerospikeTemplate operations,
                                  Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(baseQueryMethod, evalContextAccessor, queryCreator, (AerospikeMappingContext) operations.getMappingContext(),
            operations.getAerospikeConverter(), operations.getServerVersionSupport(), operations.getDSLParser());
        this.operations = operations;
        // each queryMethod here is AerospikeQueryMethod
        this.queryMethod = (AerospikeQueryMethod) baseQueryMethod;
    }

    @Override
    @SuppressWarnings({"NullableProblems"})
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Class<?> targetClass = getTargetClass(accessor);

        if (queryMethod.hasQueryAnnotation()) {
            return findByQueryAnnotation(queryMethod, targetClass, parameters);
        }
        Query query = prepareQuery(parameters, accessor);

        // queries with id equality have their own processing flow
        if (parameters != null && parameters.length > 0) {
            Qualifier criteria = query.getCriteriaObject();
            // only for id EQ, id LIKE queries have SimpleProperty query creator
            if (criteria.hasSingleId()) {
                return runQueryWithIdsEquality(targetClass, getIdValue(criteria), null);
            } else {
                Qualifier idQualifier;
                if ((idQualifier = getIdQualifier(criteria)) != null) {
                    return runQueryWithIdsEquality(targetClass, getIdValue(idQualifier),
                        new Query(excludeIdQualifier(criteria)));
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

    protected Object runQueryWithIdsEquality(Class<?> targetClass, List<Object> ids, Query query) {
        if (isExistsQuery(queryMethod)) {
            return operations.existsByIdsUsingQuery(ids, entityClass, query);
        } else if (isCountQuery(queryMethod)) {
            return operations.countByIdsUsingQuery(ids, entityClass, query);
        } else if (isDeleteQuery(queryMethod)) {
            operations.deleteByIdsUsingQuery(ids, entityClass, query);
            return Optional.empty();
        } else {
            return operations.findByIdsUsingQuery(ids, entityClass, targetClass, query);
        }
    }

    private Object processPaginatedQuery(Class<?> targetClass, Pageable pageable, Query query) {
        Stream<?> unprocessedResultsStream =
            operations.findUsingQueryWithoutPostProcessing(entityClass, targetClass, query);
        if (queryMethod.isSliceQuery()) {
            return processSliceQuery(unprocessedResultsStream, pageable, query);
        }
        return processPageQuery(unprocessedResultsStream, pageable, query);
    }

    private Object processSliceQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        if (pageable.isUnpaged()) {
            return new SliceImpl<>(unprocessedResultsStream.toList(), pageable, false);
        }

        Query modifiedQuery = query.limit(pageable.getPageSize() + 1);
        List<Object> modifiedResults = applyPostProcessing(unprocessedResultsStream, modifiedQuery)
            .collect(Collectors.toList());

        boolean hasNext = modifiedResults.size() > pageable.getPageSize();
        return new SliceImpl<>(hasNext ? modifiedResults.subList(0, pageable.getPageSize()) : modifiedResults,
            pageable, hasNext);
    }

    private Object processPageQuery(Stream<?> unprocessedResultsStream, Pageable pageable, Query query) {
        long numberOfAllResults;
        List<?> resultsPage;
        if (operations.getQueryMaxRecords() > 0) {
            // Assuming there is enough memory
            // and configuration parameter AerospikeDataSettings.queryMaxRecords is less than Integer.MAX_VALUE
            List<?> unprocessedResults = unprocessedResultsStream.toList();
            numberOfAllResults = unprocessedResults.size();
            resultsPage = pageable.isUnpaged() ? unprocessedResults : applyPostProcessing(unprocessedResults.stream(),
                query).toList();
        } else {
            numberOfAllResults = operations.count(query, entityClass);
            resultsPage = pageable.isUnpaged() ? unprocessedResultsStream.toList()
                : applyPostProcessing(unprocessedResultsStream, query).toList();
        }
        return new PageImpl<>(resultsPage, pageable, numberOfAllResults);
    }

    @Override
    protected Stream<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class.
        if (targetClass != null && targetClass != entityClass) {
            return operations.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type.
        return operations.find(query, entityClass);
    }
}
