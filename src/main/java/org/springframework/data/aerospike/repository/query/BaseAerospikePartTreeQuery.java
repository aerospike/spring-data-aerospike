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

import com.aerospike.dsl.DSLParser;
import com.aerospike.dsl.ParsedExpression;
import com.aerospike.dsl.exception.AerospikeDSLException;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.domain.Sort;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * @author Peter Milne
 * @author Jean Mercier
 * @author Igor Ermolenko
 */
public abstract class BaseAerospikePartTreeQuery<T> implements RepositoryQuery {

    protected final QueryMethod baseQueryMethod;
    protected final Class<?> entityClass;
    private final QueryMethodValueEvaluationContextAccessor evaluationContextAccessor;
    private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;
    private final AerospikeMappingContext context;
    private final MappingAerospikeConverter converter;
    private final ServerVersionSupport versionSupport;
    private final DSLParser dslParser;
    private static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("(?<!\\.)\\?(\\d+)");

    protected BaseAerospikePartTreeQuery(QueryMethod queryMethod,
                                         QueryMethodValueEvaluationContextAccessor evalContextAccessor,
                                         Class<? extends AbstractQueryCreator<?, ?>> queryCreator,
                                         AerospikeMappingContext context,
                                         MappingAerospikeConverter converter, ServerVersionSupport versionSupport,
                                         DSLParser dslParser) {
        this.baseQueryMethod = queryMethod;
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
        return baseQueryMethod;
    }

    protected Query prepareQuery(Object[] parameters, ParametersParameterAccessor accessor) {
        PartTree tree = new PartTree(baseQueryMethod.getName(), entityClass);
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
            // Create a ValueEvaluationContextProvider using the accessor
            ValueEvaluationContextProvider provider =
                this.evaluationContextAccessor.create(baseQueryMethod.getParameters());

            // Get the ValueEvaluationContext using the provider
            ValueEvaluationContext valueContext = provider.getEvaluationContext(parameters);

            // Convert to EvaluationContext using the getDelegate() method
            EvaluationContext context = valueContext.getRequiredEvaluationContext();

            // Set the context on the SpelExpression
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
        if (!isEntityAssignableFromReturnType(baseQueryMethod)) {
            return baseQueryMethod.getReturnedObjectType();
        }
        // No projection - target class will be the entity class.
        return baseQueryMethod.getEntityInformation().getJavaType();
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

    protected abstract Object runQueryWithIdsEquality(Class<?> targetClass, List<Object> ids, Query query);

    protected boolean isExistsQuery(QueryMethod queryMethod) {
        return queryMethod.getName().startsWith("existsBy");
    }

    protected boolean isCountQuery(QueryMethod queryMethod) {
        return queryMethod.getName().startsWith("countBy");
    }

    protected boolean isDeleteQuery(QueryMethod queryMethod) {
        return queryMethod.getName().startsWith("deleteBy") || queryMethod.getName().startsWith("removeBy");
    }

    /**
     * Find whether entity domain class is assignable from query method's returned object class. Not assignable when
     * using a detached DTO (data transfer object, e.g., for projections).
     *
     * @param queryMethod QueryMethod in use
     * @return true when entity is assignable from query method's return class, otherwise false
     */
    protected boolean isEntityAssignableFromReturnType(QueryMethod queryMethod) {
        return queryMethod.getEntityInformation().getJavaType().isAssignableFrom(queryMethod.getReturnedObjectType());
    }

    protected abstract T findByQuery(Query query, Class<?> targetClass);

    protected List<String> convertToStringsList(Object[] parameters) {
        if (parameters == null) {
            return new ArrayList<>();
        }

        return IntStream.range(0, parameters.length)
            .mapToObj(idx -> {
                Object obj = parameters[idx];
                if (obj == null) {
                    throw new IllegalArgumentException("Element at index " + idx + " is null");
                } else if (obj instanceof String str) {
                    // Replace any existing single quotes with escaped quotes
                    str = str.replace("'", "\\'");
                    return "'" + str + "'";
                } else if (obj instanceof Number || obj instanceof Boolean) {
                    return obj.toString();
                } else {
                    throw new IllegalArgumentException(String.format(
                        "Element at index %s is expected to be a String, number or boolean, instead got %s",
                        idx, obj.getClass().getName())
                    );
                }
            }).collect(Collectors.toList());
    }

    protected String fillPlaceholders(String input, Object[] parameters) {
        if (parameters == null || parameters.length == 0) return input;

        List<String> parametersList = convertToStringsList(parameters);
        // Use regex to find and replace placeholders
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(input);
        StringBuffer result = new StringBuffer();

        while (matcher.find()) {
            int index = Integer.parseInt(matcher.group(1));
            if (index >= 0 && index < parametersList.size()) {
                // Escape any special characters in the replacement
                String replacement = Matcher.quoteReplacement(parametersList.get(index));
                matcher.appendReplacement(result, replacement);
            } else {
                throw new AerospikeDSLException("Parameter index out of bounds: " + index);
            }
        }
        matcher.appendTail(result);
        return result.toString();
    }

    protected T findByQueryAnnotation(AerospikeQueryMethod queryMethod, Class<?> targetClass, String namespace,
                                      Map<IndexKey, Index> indexCache, Object[] parameters) {
        // Map cached indexes to DSL
        List<com.aerospike.dsl.index.Index> indexes = indexCache.values().stream().map(value ->
            com.aerospike.dsl.index.Index.builder()
                .bin(value.getBin())
                .indexType(value.getIndexType())
                .binValuesRatio(value.getBinValuesRatio())
                .build())
            .toList();
        // Parse the given expression
        ParsedExpression expr = dslParser.parseDslExpression(
            fillPlaceholders(queryMethod.getQueryAnnotation(), parameters),
            namespace,
            indexes
        );
        // Create the query
        Query query = new Query(Qualifier.filterBuilder().setFilterExpression(expr.getExpression()).build());
        return findByQuery(query, targetClass);
    }
}
