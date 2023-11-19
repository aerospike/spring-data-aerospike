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
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.SliceImpl;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.TemplateUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.core.TemplateUtils.getIdValue;
import static org.springframework.data.aerospike.query.QualifierUtils.getIdQualifier;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikePartTreeQuery extends BaseAerospikePartTreeQuery {

    private final AerospikeOperations operations;

    public AerospikePartTreeQuery(QueryMethod queryMethod,
                                  QueryMethodEvaluationContextProvider evalContextProvider,
                                  AerospikeTemplate operations,
                                  Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(queryMethod, evalContextProvider, queryCreator);

        this.operations = operations;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes", "NullableProblems"})
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
                return operations.findByIdsUsingQuery(ids, entityClass, targetClass, null);
            } else {
                Qualifier idQualifier;
                if ((idQualifier = getIdQualifier(criteria)) != null) {
                    ids = getIdValue(idQualifier);
                    return operations.findByIdsUsingQuery(ids, entityClass, targetClass,
                        new Query(excludeIdQualifier(criteria)));
                }
            }
        }

        if (queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
            Pageable pageable = accessor.getPageable();
            Stream<?> unprocessedResultsStream =
                operations.findUsingQueryWithoutPostProcessing(entityClass, targetClass, query);
            if (queryMethod.isSliceQuery()) {
                if (pageable.isUnpaged()) {
                    return new SliceImpl<>(unprocessedResultsStream.toList(), pageable, false);
                }

                Query modifiedQuery = query.limit(pageable.getPageSize() + 1);
                List<?> modifiedResults = applyPostProcessing(unprocessedResultsStream, modifiedQuery).toList();

                boolean hasNext = modifiedResults.size() > pageable.getPageSize();
                return new SliceImpl(hasNext ? modifiedResults.subList(0, pageable.getPageSize()) : modifiedResults,
                    pageable, hasNext);
            } else {
                long numberOfAllResults;
                List<?> resultsPage;
                if (operations.getQueryMaxRecords() > 0) {
                    // Assuming there is enough memory
                    // and configuration parameter AerospikeDataSettings.queryMaxRecords is less than Integer.MAX_VALUE
                    List<?> unprocessedResults = unprocessedResultsStream.toList();
                    numberOfAllResults = unprocessedResults.size();

                    if (pageable.isUnpaged()) {
                        return new PageImpl<>(unprocessedResults, pageable, numberOfAllResults);
                    }
                    resultsPage = applyPostProcessing(unprocessedResults.stream(), query).toList();
                } else {
                    numberOfAllResults = operations.count(query, entityClass);
                    resultsPage = applyPostProcessing(unprocessedResultsStream, query).toList();
                }
                return new PageImpl(resultsPage, pageable, numberOfAllResults);
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

    private Stream<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class.
        if (targetClass != null && targetClass != entityClass) {
            return operations.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type.
        return operations.find(query, entityClass);
    }
}
