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

import com.aerospike.client.exp.Exp;
import com.aerospike.client.query.Filter;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.api.DSLParser;
import org.springframework.beans.BeanUtils;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.domain.Pageable;
import org.springframework.data.expression.ValueEvaluationContext;
import org.springframework.data.expression.ValueEvaluationContextProvider;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import static org.springframework.data.aerospike.core.QualifierUtils.excludeIdQualifier;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.parseDslExpression;

/**
 * @author Peter Milne
 * @author Jean Mercier
 * @author Igor Ermolenko
 */
public abstract class BaseAerospikePartTreeQuery<T> implements RepositoryQuery {

    protected final QueryMethod queryMethod;
    protected final Class<?> entityClass;
    private final QueryMethodValueEvaluationContextAccessor evaluationContextAccessor;
    private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;
    private final AerospikeMappingContext context;
    private final MappingAerospikeConverter converter;
    private final ServerVersionSupport versionSupport;
    private final DSLParser dslParser;

    protected BaseAerospikePartTreeQuery(QueryMethod queryMethod,
                                         QueryMethodValueEvaluationContextAccessor evalContextAccessor,
                                         Class<? extends AbstractQueryCreator<?, ?>> queryCreator,
                                         AerospikeMappingContext context,
                                         MappingAerospikeConverter converter, ServerVersionSupport versionSupport,
                                         DSLParser dslParser) {
        this.queryMethod = queryMethod;
        this.evaluationContextAccessor = evalContextAccessor;
        this.queryCreator = queryCreator;
        this.entityClass = queryMethod.getEntityInformation().getJavaType();
        this.context = context;
        this.converter = converter;
        this.versionSupport = versionSupport;
        this.dslParser = dslParser;
    }

    @Override
    public QueryMethod getQueryMethod() {
        return queryMethod;
    }

    /**
     * Prepares a {@link Query} object based on the provided parameters, accessor, and query method. This method
     * constructs the query, applies pagination and sorting, handles limiting clauses, and sets up dynamic SpEL
     * expression evaluation if present in the criteria.
     *
     * @param parameters The array of parameters passed to the query method
     * @param accessor   The accessor for method parameters, providing access to pagination, sorting etc.
     * @return A new {@link Query} object
     */
    protected Query prepareQuery(Object[] parameters, ParametersParameterAccessor accessor) {
        PartTree tree = new PartTree(queryMethod.getName(), entityClass);
        Query baseQuery = createQuery(accessor, tree);

        Qualifier criteria = baseQuery.getCriteriaObject();
        Query query = new Query(criteria);

        if (accessor.getPageable().isPaged()) {
            query.setOffset(accessor.getPageable().getOffset());
            query.setRows(accessor.getPageable().getPageSize());
        } else {
            if (tree.isLimiting() && tree.getMaxResults() != null) { // whether it contains "first"/"top"
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
            // Create a ValueEvaluationContextProvider using the accessor
            ValueEvaluationContextProvider provider =
                this.evaluationContextAccessor.create(queryMethod.getParameters());

            // Get the ValueEvaluationContext using the provider
            ValueEvaluationContext valueContext = provider.getEvaluationContext(parameters);

            // Convert to EvaluationContext using the getDelegate() method
            EvaluationContext context = valueContext.getRequiredEvaluationContext();

            // Set the context on the SpelExpression
            spelExpression.setEvaluationContext(context);
        }

        return query;
    }

    /**
     * Creates a {@link Query} instance using a {@link AbstractQueryCreator}. This method instantiates the query creator
     * with the necessary components and then delegates to it to create the actual query.
     *
     * @param accessor The accessor for method parameters
     * @param tree     The {@link PartTree} representing the parsed query method name
     * @return A {@link Query} object constructed by the query creator
     */
    public Query createQuery(ParametersParameterAccessor accessor, PartTree tree) {
        Constructor<? extends AbstractQueryCreator<?, ?>> constructor = ClassUtils
            .getConstructorIfAvailable(queryCreator, PartTree.class, ParameterAccessor.class,
                AerospikeMappingContext.class, MappingAerospikeConverter.class, ServerVersionSupport.class);
        return (Query) BeanUtils.instantiateClass(constructor, tree, accessor, context, converter, versionSupport)
            .createQuery();
    }

    /**
     * Abstract method to run a query with a list of IDs for equality. Implementations should define how to execute the
     * query given the target class, a list of IDs, the query object, and whether it's a paged query.
     *
     * @param targetClass  The target class for the query
     * @param ids          The list of IDs to use for equality comparison in the query
     * @param query        The {@link Query} object to execute
     * @param isPagedQuery A {@link Pageable} object indicating if the query is paged
     * @return An {@link Object} representing the result of the query execution
     */
    protected abstract Object runQueryWithIdsEquality(Class<?> targetClass, List<Object> ids, Query query,
                                                      Pageable isPagedQuery);

    /**
     * Determines the target class for the query's projection. This can be:
     * <ul>
     * <li>a dynamic projection class if specified in the accessor;</li>
     * <li>a DTO projection class if the return type of the query method is not assignable from the entity class;</li>
     * <li>the entity class itself if no projection is specified</li>
     * </ul>
     *
     * @param accessor The accessor for method parameters
     * @return The {@link Class} representing the target type for the query results
     */
    static Class<?> getTargetClass(ParametersParameterAccessor accessor, QueryMethod queryMethod) {
        // Dynamic projection
        if (accessor.getParameters().hasDynamicProjection()) {
            return accessor.findDynamicProjection();
        }
        // DTO projection
        if (!isEntityAssignableFromReturnType(queryMethod)) {
            return queryMethod.getReturnedObjectType();
        }
        // No projection - target class will be the entity class
        return queryMethod.getEntityInformation().getJavaType();
    }

    /**
     * Checks if the given {@link QueryMethod} represents an "exists by" query.
     *
     * @param queryMethod The {@link QueryMethod} to check
     * @return {@code true} if the query method name starts with "existsBy", {@code false} otherwise
     */
    protected static boolean isExistsQuery(QueryMethod queryMethod) {
        return queryMethod.getName().startsWith("existsBy");
    }

    /**
     * Checks if the given {@link QueryMethod} represents a "count by" query.
     *
     * @param queryMethod The {@link QueryMethod} to check
     * @return {@code true} if the query method name starts with "countBy", {@code false} otherwise
     */
    protected static boolean isCountQuery(QueryMethod queryMethod) {
        return queryMethod.getName().startsWith("countBy");
    }

    /**
     * Checks if the given {@link QueryMethod} represents a "delete by" query.
     *
     * @param queryMethod The {@link QueryMethod} to check
     * @return {@code true} if the query method name starts with "deleteBy" or "removeBy", {@code false} otherwise
     */
    protected static boolean isDeleteQuery(QueryMethod queryMethod) {
        return queryMethod.getName().startsWith("deleteBy") || queryMethod.getName().startsWith("removeBy");
    }

    /**
     * Find whether entity domain class is assignable from query method's returned object class. Not assignable when
     * using a detached DTO (data transfer object, e.g., for projections).
     *
     * @param queryMethod QueryMethod in use
     * @return true when entity is assignable from query method's return class, otherwise false
     */
    protected static boolean isEntityAssignableFromReturnType(QueryMethod queryMethod) {
        return queryMethod.getEntityInformation().getJavaType().isAssignableFrom(queryMethod.getReturnedObjectType());
    }

    /**
     * Creates a new {@link Query} object by excluding the ID qualifier from the original criteria and retaining the
     * original query's sort, offset, rows, and distinct settings.
     *
     * @param query    The original {@link Query} from which to derive the new query
     * @param criteria The {@link Qualifier} from which to exclude the ID qualifier
     * @return A new {@link Query} object with the ID qualifier excluded from its criteria
     */
    protected static Query getQueryWithExcludedIdQualifier(Query query, Qualifier criteria) {
        Query newQuery = new Query(excludeIdQualifier(criteria));
        newQuery.setSort(query.getSort());
        newQuery.setOffset(query.getOffset());
        newQuery.setRows(query.getRows());
        newQuery.setDistinct(query.isDistinct());
        return newQuery;
    }

    protected abstract T findByQuery(Query query, Class<?> targetClass);

    protected T findByQueryAnnotation(AerospikeQueryMethod queryMethod, Class<?> targetClass, String namespace,
                                      Map<IndexKey, Index> indexCache, Object[] parameters) {

        // Parse the given expression
        ParsedExpression expr =
            parseDslExpression(queryMethod.getQueryAnnotation(), namespace, indexCache, parameters, dslParser);

        // Create the query
        Filter filter = expr.getResult().getFilter();
        Exp exp = expr.getResult().getExp();
        Query query = new Query(
            Qualifier.filterBuilder()
                .setFilter(filter)
                .setExpression(exp == null ? null : Exp.build(exp))
                .build()
        );
        return findByQuery(query, targetClass);
    }
}
