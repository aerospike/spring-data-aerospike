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
import org.springframework.data.aerospike.convert.AerospikeCustomConversions;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMapCriteria;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.util.TypeInformation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.springframework.data.aerospike.query.FilterOperation.LIST_VAL_CONTAINING;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_KEYS_CONTAIN;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VALUES_CONTAIN;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VAL_CONTAINING_BY_KEY;
import static org.springframework.data.aerospike.query.FilterOperation.MAP_VAL_EQ_BY_KEY;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeQueryCreator extends AbstractQueryCreator<Query, AerospikeCriteria> {

    private static final Logger LOG = LoggerFactory.getLogger(AerospikeQueryCreator.class);
    private final AerospikeMappingContext context;
    private final AerospikeCustomConversions conversions = new AerospikeCustomConversions(Collections.emptyList());
    private final MappingAerospikeConverter converter = getMappingAerospikeConverter(conversions);

    public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters) {
        super(tree, parameters);
        this.context = new AerospikeMappingContext();
    }

    public AerospikeQueryCreator(PartTree tree, ParameterAccessor parameters,
                                 AerospikeMappingContext context) {
        super(tree, parameters);
        this.context = context;
    }

    private MappingAerospikeConverter getMappingAerospikeConverter(AerospikeCustomConversions conversions) {
        MappingAerospikeConverter converter = new MappingAerospikeConverter(new AerospikeMappingContext(),
            conversions, new AerospikeTypeAliasAccessor());
        converter.afterPropertiesSet();
        return converter;
    }

    @Override
    protected AerospikeCriteria create(Part part, Iterator<Object> iterator) {
        PersistentPropertyPath<AerospikePersistentProperty> path =
            context.getPersistentPropertyPath(part.getProperty());
        AerospikePersistentProperty property = path.getLeafProperty();
        return create(part, property, iterator);
    }

    private AerospikeCriteria create(Part part, AerospikePersistentProperty property, Iterator<?> parameters) {
        Object v1 = parameters.next();

        // converting if necessary (e.g., Date to Long so that proper filter expression or sIndex filter can be built)
        final Object value = v1;
        if (v1 != null) {
            Optional<Class<?>> basicTargetType = conversions.getCustomWriteTarget(v1.getClass());
            v1 = basicTargetType
                .<Object>map(aClass -> converter.getConversionService().convert(value, aClass))
                .orElse(v1);
        }

        return switch (part.getType()) {
            case AFTER, GREATER_THAN -> getCriteria(part, property, v1, null, parameters, FilterOperation.GT);
            case GREATER_THAN_EQUAL -> getCriteria(part, property, v1, null, parameters, FilterOperation.GTEQ);
            case BEFORE, LESS_THAN -> getCriteria(part, property, v1, null, parameters, FilterOperation.LT);
            case LESS_THAN_EQUAL -> getCriteria(part, property, v1, null, parameters, FilterOperation.LTEQ);
            case BETWEEN -> getCriteria(part, property, v1, parameters.next(), parameters, FilterOperation.BETWEEN);
            case LIKE -> getCriteria(part, property, v1, null, parameters, FilterOperation.LIKE);
            case STARTING_WITH -> getCriteria(part, property, v1, null, parameters, FilterOperation.STARTS_WITH);
            case ENDING_WITH -> getCriteria(part, property, v1, null, parameters, FilterOperation.ENDS_WITH);
            case CONTAINING -> getCriteria(part, property, v1, null, parameters, FilterOperation.CONTAINING);
            case WITHIN -> {
                v1 = Value.get(String.format("{ \"type\": \"AeroCircle\", \"coordinates\": [[%.8f, %.8f], %f] }",
                    v1, parameters.next(), parameters.next()));
                yield getCriteria(part, property, v1, parameters.next(), parameters, FilterOperation.GEO_WITHIN);
            }
            case SIMPLE_PROPERTY -> getCriteria(part, property, v1, null, parameters, FilterOperation.EQ);
            case NEGATING_SIMPLE_PROPERTY -> getCriteria(part, property, v1, null, parameters, FilterOperation.NOTEQ);
            case IN -> getCriteria(part, property, v1, null, parameters, FilterOperation.IN);
            default -> throw new IllegalArgumentException("Unsupported keyword '" + part.getType() + "'");
        };
    }

    public AerospikeCriteria getCriteria(Part part, AerospikePersistentProperty property, Object value1, Object value2,
                                         Iterator<?> parameters, FilterOperation op) {
        Qualifier.QualifierBuilder qb = new Qualifier.QualifierBuilder();
        String fieldName = part.getProperty().getSegment(); // Map bin name, later passed to Exp.mapBin()
        String dotPath = null;
        Object value3 = null;

        if (property.isCollectionLike()) {
            List<Object> params = new ArrayList<>();
            parameters.forEachRemaining(params::add);

            if (!params.isEmpty()) {
                Object nextParam = params.get(params.size() - 1);
                if (op == FilterOperation.CONTAINING && !(nextParam instanceof AerospikeMapCriteria)) {
                    op = LIST_VAL_CONTAINING;
                    params.add(0, value1); // value1 stores the first parameter
                    return aerospikeCriteriaAndConcatenated(params, qb, part, fieldName, op, dotPath);
                }
            } else if (!(value1 instanceof Collection<?>)) { // preserving the initial FilterOperation if Collection
                op = getCorrespondingListFilterOperationOrFail(op);
            }
        } else if (property.isMap()) {
            List<Object> params = new ArrayList<>();
            parameters.forEachRemaining(params::add);

            if (params.size() == 1) { // more than 1 parameter (values) provided, the first is stored in value1
                Object nextParam = params.get(0);
                if (op == FilterOperation.CONTAINING) {
                    if (nextParam instanceof AerospikeMapCriteria onMap) {
                        switch (onMap) {
                            case KEY -> op = MAP_KEYS_CONTAIN;
                            case VALUE -> op = MAP_VALUES_CONTAIN;
                        }
                    } else {
                        op = FilterOperation.MAP_VAL_EQ_BY_KEY;
                        dotPath = part.getProperty().toDotPath() + "." + Value.get(value1);
                        setQbValuesForMapByKey(qb, value1, nextParam);
                    }
                } else {
                    if (op == FilterOperation.BETWEEN) { // BETWEEN for values by a certain key
                        op = getCorrespondingMapValueFilterOperationOrFail(op);
                        qb.setValue2(Value.get(value1)); // contains key
                        qb.setValue1(Value.get(value2)); // contains lower limit (inclusive)
                        qb.setValue3(Value.get(nextParam)); // contains upper limit (inclusive)
                    } else {
                        if (op == FilterOperation.EQ) {
                            throw new IllegalArgumentException("Unsupported arguments '" + value1 + "' and '" + nextParam +
                                "', expecting Map argument in findByMapEquals queries");
                        } else {
                            op = getCorrespondingMapValueFilterOperationOrFail(op);
                            setQbValuesForMapByKey(qb, value1, nextParam);
                        }
                    }
                    dotPath = part.getProperty().toDotPath() + "." + Value.get(value1);
                }
            } else if (params.isEmpty()) {
                if (op != FilterOperation.BETWEEN) { // if not map in range (2 maps as parameters)
                    // VALUE2 contains key (field name)
                    value2 = Value.get(property.getFieldName());
                }
            } else {
                if (op == FilterOperation.CONTAINING) {
                    if (params.get(params.size() - 1) instanceof AerospikeMapCriteria onMap) {
                        switch (onMap) {
                            case KEY -> op = MAP_KEYS_CONTAIN;
                            case VALUE -> op = MAP_VALUES_CONTAIN;
                            case VALUE_CONTAINING -> op = MAP_VAL_CONTAINING_BY_KEY;
                        }
                        params = params.stream().limit(params.size() - 1).collect(Collectors.toList());
                    } else {
                        op = FilterOperation.MAP_VAL_EQ_BY_KEY;
                        dotPath = part.getProperty().toDotPath() + "." + Value.get(value1);
                    }

                    params.add(0, value1); // value1 stores the first parameter
                    if (op == MAP_VAL_CONTAINING_BY_KEY || op == MAP_VAL_EQ_BY_KEY) {
                        if (params.size() > 2) {
                            if ((params.size() & 1) != 0) { // if params.size() is an odd number
                                throw new IllegalArgumentException("FindByMapContaining: expected either 1, 2 " +
                                    "or even number of key/value arguments, instead got " + params.size());
                            }
                            return aerospikeCriteriaAndConcatenated(params, qb, part, fieldName, op, dotPath, true);
                        } else if (params.size() == 2) {
                            setQbValuesForMapByKey(qb, params.get(0), params.get(1));
                        }
                    } else {
                        return aerospikeCriteriaAndConcatenated(params, qb, part, fieldName, op, dotPath);
                    }
                } else {
                    String paramsString = params.stream().map(Object::toString).collect(Collectors.joining(", "));
                    throw new IllegalArgumentException(
                        "Expected not more than 2 arguments (propertyType: Map, filterOperation: " + op + "), " +
                            " got " + (params.size() + 1) + " instead: '" + value1 + ", " + paramsString + "'");
                }
            }
        } else { // if it is neither a collection nor a map
            if (part.getProperty().hasNext()) { // if it is a POJO field (a simple field or an inner POJO)
                if (op == FilterOperation.BETWEEN) {
                    value3 = Value.get(value2); // contains upper limit
                }
                op = getCorrespondingMapValueFilterOperationOrFail(op);
                value2 = Value.get(property.getFieldName()); // VALUE2 contains key (field name)
                dotPath = part.getProperty().toDotPath();
            } else if (isPojo(part)) { // if it is a first level POJO
                if (op != FilterOperation.BETWEEN) {
                    // if it is a POJO compared for equality it already has op == FilterOperation.EQ
                    value2 = Value.get(property.getFieldName()); // VALUE2 contains key (field name)
                }
            }
        }

        return new AerospikeCriteria(
            setQualifierBuilderValues(qb, fieldName, op, part, value1, value2, value3, dotPath)
        );
    }


    private AerospikeCriteria aerospikeCriteriaAndConcatenated(List<Object> params, Qualifier.QualifierBuilder qb,
                                                               Part part, String fieldName, FilterOperation op,
                                                               String dotPath) {
        return aerospikeCriteriaAndConcatenated(params, qb, part, fieldName, op, dotPath, false);
    }

    private AerospikeCriteria aerospikeCriteriaAndConcatenated(List<Object> params, Qualifier.QualifierBuilder qb,
                                                               Part part, String fieldName, FilterOperation op,
                                                               String dotPath, boolean containingMapKeyValuePairs) {
        Qualifier[] qualifiers;
        if (containingMapKeyValuePairs) {
            qualifiers = new Qualifier[params.size() / 2]; // keys/values qty must be even
            for (int i = 0, j = 0; i < params.size(); i += 2) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i + 1));
                Qualifier qualifier = setQualifierBuilderValues(qb, fieldName, op, part, params.get(i),
                    null, null, dotPath).build();
                qualifiers[j++] = qualifier;
            }

            return new AerospikeCriteria(
                new Qualifier.QualifierBuilder()
                    .setQualifiers(qualifiers)
                    .setFilterOperation(FilterOperation.AND)
            );
        } else {
            qualifiers = new Qualifier[params.size()];
            for (int i = 0; i < params.size(); i++) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i));
                Qualifier qualifier = setQualifierBuilderValues(qb, fieldName, op, part, params.get(i),
                    null, null, dotPath).build();
                qualifiers[i] = qualifier;
            }
        }

        return new AerospikeCriteria(
            new Qualifier.QualifierBuilder()
                .setQualifiers(qualifiers)
                .setFilterOperation(FilterOperation.AND)
        );
    }

    private Qualifier.QualifierBuilder setQualifierBuilderValues(Qualifier.QualifierBuilder qb, String fieldName,
                                                                 FilterOperation op, Part part, Object value1,
                                                                 Object value2, Object value3, String dotPath) {
        qb.setField(fieldName)
            .setFilterOperation(op)
            .setIgnoreCase(ignoreCaseToBoolean(part))
            .setConverter(converter);
        setNotNullQbValues(qb, value1, value2, value3, dotPath);
        return qb;
    }

    private FilterOperation getCorrespondingMapValueFilterOperationOrFail(FilterOperation op) {
        try {
            return FilterOperation.valueOf("MAP_VAL_" + op + "_BY_KEY");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot find corresponding MAP_VAL_..._BY_KEY FilterOperation for '" + op + "'");
        }
    }

    private FilterOperation getCorrespondingListFilterOperationOrFail(FilterOperation op) {
        try {
            return FilterOperation.valueOf("LIST_VAL_" + op);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "Cannot find corresponding LIST_VAL_... FilterOperation for '" + op + "'");
        }
    }

    private void setNotNullQbValues(Qualifier.QualifierBuilder qb, Object v1, Object v2, Object v3, String dotPath) {
        if (v1 != null && !qb.hasValue1()) qb.setValue1(Value.get(v1));
        if (v2 != null && !qb.hasValue2()) qb.setValue2(Value.get(v2));
        if (v3 != null && !qb.hasValue3()) qb.setValue3(Value.get(v3));
        if (dotPath != null && !qb.hasDotPath()) qb.setDotPath(dotPath);
    }

    private void setQbValuesForMapByKey(Qualifier.QualifierBuilder qb, Object key, Object value) {
        qb.setValue1(Value.get(value)); // contains value
        qb.setValue2(Value.get(key)); // contains key
    }

    private boolean isPojo(Part part) { // if it is a first level POJO
        TypeInformation<?> type = TypeInformation.of(part.getProperty().getType());
        // returns true if it is a POJO or a Map
        return !conversions.isSimpleType(part.getProperty().getType()) && !type.isCollectionLike();
    }

    @Override
    protected AerospikeCriteria and(Part part, AerospikeCriteria base, Iterator<Object> iterator) {
        if (base == null) {
            return create(part, iterator);
        }

        PersistentPropertyPath<AerospikePersistentProperty> path =
            context.getPersistentPropertyPath(part.getProperty());
        AerospikePersistentProperty property = path.getLeafProperty();

        return new AerospikeCriteria(new Qualifier.QualifierBuilder()
            .setFilterOperation(FilterOperation.AND)
            .setQualifiers(base, create(part, property, iterator))
        );
    }

    @Override
    protected AerospikeCriteria or(AerospikeCriteria base, AerospikeCriteria criteria) {
        return new AerospikeCriteria(new Qualifier.QualifierBuilder()
            .setFilterOperation(FilterOperation.OR)
            .setQualifiers(base, criteria)
        );
    }

    @Override
    protected Query complete(AerospikeCriteria criteria, Sort sort) {
        Query query = criteria == null ? null : new Query(criteria).with(sort);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Created query {}", query);
        }

        return query;
    }

    private boolean ignoreCaseToBoolean(Part part) {
        return switch (part.shouldIgnoreCase()) {
            case WHEN_POSSIBLE -> part.getProperty().getType() == String.class;
            case ALWAYS -> true;
            default -> false;
        };
    }
}
