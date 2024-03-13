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

import com.aerospike.client.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.springframework.data.aerospike.query.FilterOperation.IS_NOT_NULL;
import static org.springframework.data.aerospike.query.FilterOperation.IS_NULL;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.convertIfNecessary;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getFieldName;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getNestedPropertyPath;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.isPojo;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreator extends AbstractQueryCreator<Query, CriteriaDefinition> {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeQueryCreator.class);
    private final AerospikeMappingContext context;
    private final MappingAerospikeConverter converter;
    private final boolean isCombinedQuery;

    public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters,
                                 AerospikeMappingContext context, MappingAerospikeConverter converter) {
        super(tree, parameters);
        this.context = context;
        this.converter = converter;
        this.isCombinedQuery = tree.getParts().toList().size() > 1;
    }

    @Override
    protected CriteriaDefinition and(Part part, CriteriaDefinition base, Iterator<Object> iterator) {
        CriteriaDefinition criteriaDefinition = create(part, iterator);

        if (base == null) {
            return criteriaDefinition;
        }

        return Qualifier.and(base.getCriteriaObject(), criteriaDefinition.getCriteriaObject());
    }

    @Override
    protected CriteriaDefinition or(CriteriaDefinition base, CriteriaDefinition criteria) {
        return Qualifier.or(base.getCriteriaObject(), criteria.getCriteriaObject());
    }

    @Override
    protected Query complete(CriteriaDefinition criteria, Sort sort) {
        Query query = criteria == null ? null : new Query(criteria).with(sort);

        if (LOG.isDebugEnabled() && criteria != null) {
            logQualifierDetails(criteria);
        }

        return query;
    }

    @Override
    protected CriteriaDefinition create(Part part, Iterator<Object> iterator) {
        PersistentPropertyPath<AerospikePersistentProperty> path =
            context.getPersistentPropertyPath(part.getProperty());
        AerospikePersistentProperty property = path.getLeafProperty();
        Iterator<Object> paramIterator = iterator;

        if (isCombinedQuery && iterator.hasNext()) {
            Object nextParam = iterator.next();
            if (!(nextParam instanceof QueryParam)) {
                throw new IllegalArgumentException(String.format("%s: expected CombinedQueryParam, instead got %s",
                    part.getProperty(), nextParam.getClass().getSimpleName()));
            }
            paramIterator = Arrays.stream(((QueryParam) nextParam).arguments()).iterator();
        }

        return create(part, property, paramIterator);
    }

    private CriteriaDefinition create(Part part, AerospikePersistentProperty property, Iterator<?> parameters) {
        List<Object> queryParameters = getQueryParameters(parameters);
        FilterOperation filterOperation = getFilterOperation(part.getType());
        IAerospikeQueryCreator queryCreator = getQueryCreator(part, property, queryParameters, filterOperation);

        queryCreator.validate();
        return queryCreator.process();
    }

    private IAerospikeQueryCreator getQueryCreator(Part part, AerospikePersistentProperty property,
                                                   List<Object> queryParameters, FilterOperation filterOperation) {
        String fieldName = getFieldName(part.getProperty().getSegment(), property);
        IAerospikeQueryCreator queryCreator;

        if (property.isIdProperty()) {
            queryCreator = new IdQueryCreator(queryParameters);
        } else if (property.isCollectionLike()) {
            queryCreator = new CollectionQueryCreator(part, property, fieldName, queryParameters, filterOperation, converter);
        } else if (property.isMap()) {
            queryCreator = new MapQueryCreator(part, property, fieldName, queryParameters, filterOperation, converter);
        } else {
            if (part.getProperty().hasNext()) { // a POJO field (a simple property field or an inner POJO)
                PropertyPath nestedProperty = getNestedPropertyPath(part.getProperty());
                if (isPojo(nestedProperty.getType())) {
                    queryCreator = new PojoQueryCreator(part, nestedProperty, property, fieldName, queryParameters,
                        filterOperation, converter, true);
                } else {
                    queryCreator = new SimplePropertyQueryCreator(part, nestedProperty, property, fieldName,
                        queryParameters, filterOperation, converter, true);
                }
            } else if (isPojo(part.getProperty().getType())) { // a first level POJO or a Map
                queryCreator = new PojoQueryCreator(part, part.getProperty(), property, fieldName, queryParameters,
                    filterOperation, converter, false);
            } else {
                queryCreator = new SimplePropertyQueryCreator(part, part.getProperty(), property, fieldName,
                    queryParameters, filterOperation, converter, false);
            }
        }

        return queryCreator;
    }

    private FilterOperation getFilterOperation(Part.Type type) {
        return switch (type) {
            case AFTER, GREATER_THAN -> FilterOperation.GT;
            case GREATER_THAN_EQUAL -> FilterOperation.GTEQ;
            case BEFORE, LESS_THAN -> FilterOperation.LT;
            case LESS_THAN_EQUAL -> FilterOperation.LTEQ;
            case BETWEEN -> FilterOperation.BETWEEN;
            case LIKE, REGEX -> FilterOperation.LIKE;
            case STARTING_WITH -> FilterOperation.STARTS_WITH;
            case ENDING_WITH -> FilterOperation.ENDS_WITH;
            case CONTAINING -> FilterOperation.CONTAINING;
            case NOT_CONTAINING -> FilterOperation.NOT_CONTAINING;
            case WITHIN -> FilterOperation.GEO_WITHIN;
            case SIMPLE_PROPERTY, TRUE, FALSE -> FilterOperation.EQ;
            case NEGATING_SIMPLE_PROPERTY -> FilterOperation.NOTEQ;
            case IN -> FilterOperation.IN;
            case NOT_IN -> FilterOperation.NOT_IN;
            case EXISTS, IS_NOT_NULL -> IS_NOT_NULL;
            case IS_NULL -> IS_NULL;
            default -> throw new IllegalArgumentException(String.format("Unsupported keyword '%s'", type));
        };
    }

    private List<Object> getQueryParameters(Iterator<?> parametersIterator) {
        List<Object> params = new ArrayList<>();
        parametersIterator.forEachRemaining(param -> params.add(convertIfNecessary(param, converter)));
        // null parameters are not allowed, instead AerospikeNullQueryCriteria.NULL_PARAM should be used
        return params.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    private void logQualifierDetails(CriteriaDefinition criteria) {
        Qualifier qualifier = criteria.getCriteriaObject();
        Qualifier[] qualifiers = qualifier.getQualifiers();
        if (qualifiers != null && qualifiers.length > 0) {
            Arrays.stream(qualifiers).forEach(this::logQualifierDetails);
        }

        String field = (StringUtils.hasLength(qualifier.getField()) ? qualifier.getField() : "");
        String operation = qualifier.getOperation().toString();
        operation = (StringUtils.hasLength(operation) ? operation : "N/A");
        Value k = qualifier.getKey();
        String key = printValue(k);
        Value val = qualifier.getValue();
        String value = printValue(val);
        Value val2 = qualifier.getSecondValue();
        String value2 = printValue(val2);

        LOG.debug("Created query: field = {}, operation = {}, key = {}, value = {}, value2 = {}",
            field, operation, key, value, value2);
    }

    private String printValue(Value value) {
        if (value != null && StringUtils.hasLength(value.toString())) return value.toString();
        return value == Value.getAsNull() ? "null" : "";
    }
}
