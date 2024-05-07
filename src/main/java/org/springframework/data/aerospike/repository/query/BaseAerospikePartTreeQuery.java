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

import org.springframework.beans.BeanUtils;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * @author Peter Milne
 * @author Jean Mercier
 * @author Igor Ermolenko
 */
public abstract class BaseAerospikePartTreeQuery implements RepositoryQuery {

    protected final QueryMethod queryMethod;
    protected final Class<?> entityClass;
    private final QueryMethodEvaluationContextProvider evaluationContextProvider;
    private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;
    private final AerospikeMappingContext context;
    private final MappingAerospikeConverter converter;
    private final ServerVersionSupport versionSupport;

    protected BaseAerospikePartTreeQuery(QueryMethod queryMethod,
                                         QueryMethodEvaluationContextProvider evalContextProvider,
                                         Class<? extends AbstractQueryCreator<?, ?>> queryCreator,
                                         AerospikeMappingContext context,
                                         MappingAerospikeConverter converter, ServerVersionSupport versionSupport) {
        this.queryMethod = queryMethod;
        this.evaluationContextProvider = evalContextProvider;
        this.queryCreator = queryCreator;
        this.entityClass = queryMethod.getEntityInformation().getJavaType();
        this.context = context;
        this.converter = converter;
        this.versionSupport = versionSupport;
    }

    @Override
    public QueryMethod getQueryMethod() {
        return queryMethod;
    }

    protected Query prepareQuery(Object[] parameters, ParametersParameterAccessor accessor) {
        PartTree tree = new PartTree(queryMethod.getName(), entityClass);
        Query baseQuery = createQuery(accessor, tree);

        Qualifier criteria = baseQuery.getCriteriaObject();
        Query query = new Query(criteria);

        if (accessor.getPageable().isPaged()) {
            query.setOffset(accessor.getPageable().getOffset());
            query.setRows(accessor.getPageable().getPageSize());
        } else {
            if (tree.isLimiting()) { // whether it contains "first"/"top"
                query.limit(tree.getMaxResults());
            } else {
                query.setOffset(-1);
                query.setRows(-1);
            }
        }

        query.setDistinct(tree.isDistinct());

        if (accessor.getSort().isSorted()) {
            query.setSort(accessor.getSort());
        } else {
            query.setSort(baseQuery.getSort());
        }

        if (query.getCriteria() instanceof SpelExpression spelExpression) {
            EvaluationContext context = this.evaluationContextProvider.getEvaluationContext(queryMethod.getParameters(),
                parameters);
            spelExpression.setEvaluationContext(context);
        }

        return query;
    }

    Class<?> getTargetClass(ParametersParameterAccessor accessor) {
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

    public Query createQuery(ParametersParameterAccessor accessor, PartTree tree) {
        Constructor<? extends AbstractQueryCreator<?, ?>> constructor = ClassUtils
            .getConstructorIfAvailable(queryCreator, PartTree.class, ParameterAccessor.class,
                AerospikeMappingContext.class, MappingAerospikeConverter.class, ServerVersionSupport.class);
        return (Query) BeanUtils.instantiateClass(constructor, tree, accessor, context, converter, versionSupport)
            .createQuery();
    }

    protected <T> Stream<T> applyPostProcessing(Stream<T> results, Query query) {
        if (query.getSort() != null && query.getSort().isSorted()) {
            Comparator<T> comparator = getComparator(query);
            results = results.sorted(comparator);
        }
        if (query.hasOffset()) {
            results = results.skip(query.getOffset());
        }
        if (query.hasRows()) {
            results = results.limit(query.getRows());
        }

        return results;
    }

    protected <T> Comparator<T> getComparator(Query query) {
        return query.getSort().stream()
            .map(this::<T>getPropertyComparator)
            .reduce(Comparator::thenComparing)
            .orElseThrow(() -> new IllegalStateException("Comparator can not be created if sort orders are empty"));
    }

    private <T> Comparator<T> getPropertyComparator(Sort.Order order) {
        boolean ignoreCase = true;
        boolean ascending = order.getDirection().isAscending();
        return new PropertyComparator<>(order.getProperty(), ignoreCase, ascending);
    }
}
