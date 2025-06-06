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

import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.QueryParam;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.convertIfNecessary;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getFieldName;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.getNestedPropertyPath;
import static org.springframework.data.aerospike.repository.query.AerospikeQueryCreatorUtils.isPojo;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreator extends AbstractQueryCreator<Query, CriteriaDefinition> {

    private final AerospikeMappingContext context;
    private final MappingAerospikeConverter converter;
    private final ServerVersionSupport versionSupport;
    private final boolean isCombinedQuery;

    public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters, AerospikeMappingContext context,
                                 MappingAerospikeConverter converter, ServerVersionSupport versionSupport) {
        super(tree, parameters);
        this.context = context;
        this.converter = converter;
        this.versionSupport = versionSupport;
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
        FilterOperation filterOperation = getFilterOperation(part.getType());
        List<Object> queryParameters = getQueryParameters(parameters, filterOperation);
        // In case of byte[] it does not get converted to an ArrayList, so queryParameters contain byte array
        IAerospikeQueryCreator queryCreator = getQueryCreator(part, property, queryParameters, filterOperation);

        queryCreator.validate();
        return queryCreator.process();
    }

    private IAerospikeQueryCreator getQueryCreator(Part part, AerospikePersistentProperty property,
                                                   List<Object> queryParameters, FilterOperation filterOperation) {
        String fieldName = getFieldName(part.getProperty().getSegment(), property);
        IAerospikeQueryCreator queryCreator;

        if (property.isIdProperty()) {
            if (part.getType() == Part.Type.SIMPLE_PROPERTY) {
                queryCreator = new IdQueryCreator(part, queryParameters);
            } else {
                queryCreator = new SimplePropertyQueryCreator(part, part.getProperty(), property, fieldName,
                    queryParameters, filterOperation, converter, false, versionSupport);
            }
        } else if (property.isCollectionLike()) {
            if (part.getProperty().hasNext()) { // a POJO field
                PropertyPath nestedProperty = getNestedPropertyPath(part.getProperty());
                queryCreator = new CollectionQueryCreator(part, nestedProperty, property, fieldName, queryParameters,
                    filterOperation, converter, true, versionSupport);
            } else {
                queryCreator = new CollectionQueryCreator(part, part.getProperty(), property, fieldName,
                    queryParameters, filterOperation, converter, false, versionSupport);
            }
        } else if (property.isMap()) {
            if (part.getProperty().hasNext()) { // a POJO field
                PropertyPath nestedProperty = getNestedPropertyPath(part.getProperty());
                queryCreator = new MapQueryCreator(part, nestedProperty, property, fieldName, queryParameters,
                    filterOperation, converter, versionSupport, true);
            } else {
                queryCreator = new MapQueryCreator(part, part.getProperty(), property, fieldName, queryParameters,
                    filterOperation,
                    converter, versionSupport, false);
            }
        } else {
            if (part.getProperty().hasNext()) { // a POJO field (a simple property field or an inner POJO)
                PropertyPath nestedProperty = getNestedPropertyPath(part.getProperty());
                if (isPojo(nestedProperty.getType())) {
                    queryCreator = new PojoQueryCreator(part, nestedProperty, property, fieldName, queryParameters,
                        filterOperation, converter, true, versionSupport);
                } else {
                    queryCreator = new SimplePropertyQueryCreator(part, nestedProperty, property, fieldName,
                        queryParameters, filterOperation, converter, true, versionSupport);
                }
            } else if (isPojo(part.getProperty().getType())) { // a first level POJO or a Map
                queryCreator = new PojoQueryCreator(part, part.getProperty(), property, fieldName, queryParameters,
                    filterOperation, converter, false, versionSupport);
            } else {
                queryCreator = new SimplePropertyQueryCreator(part, part.getProperty(), property, fieldName,
                    queryParameters, filterOperation, converter, false, versionSupport);
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
            case EXISTS, IS_NOT_NULL -> FilterOperation.IS_NOT_NULL;
            case IS_NULL -> FilterOperation.IS_NULL;
            default -> throw new IllegalArgumentException(String.format("Unsupported keyword '%s'", type));
        };
    }

    private List<Object> getQueryParameters(Iterator<?> parametersIterator, FilterOperation filterOperation) {
        List<Object> params = new ArrayList<>();
        parametersIterator.forEachRemaining(param -> params.add(convertIfNecessary(param, converter)));
        // null parameters are not allowed, instead AerospikeNullQueryCriteria.NULL_PARAM should be used
        return params.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }
}
