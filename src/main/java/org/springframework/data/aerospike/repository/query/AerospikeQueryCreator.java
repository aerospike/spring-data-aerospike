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
import org.springframework.data.aerospike.config.AerospikeDataSettings;
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
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.data.aerospike.query.FilterOperation.*;
import static org.springframework.data.aerospike.query.Qualifier.idEquals;
import static org.springframework.data.aerospike.query.Qualifier.idIn;

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
            conversions, new AerospikeTypeAliasAccessor(), AerospikeDataSettings.builder().build());
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
        Object v1 = null;
        if (parameters.hasNext()) {
            v1 = parameters.next();
        }

        v1 = convertIfNecessary(v1);

        return switch (part.getType()) {
            case AFTER, GREATER_THAN -> getCriteria(part, property, v1, null, parameters, FilterOperation.GT);
            case GREATER_THAN_EQUAL -> getCriteria(part, property, v1, null, parameters, FilterOperation.GTEQ);
            case BEFORE, LESS_THAN -> getCriteria(part, property, v1, null, parameters, FilterOperation.LT);
            case LESS_THAN_EQUAL -> getCriteria(part, property, v1, null, parameters, FilterOperation.LTEQ);
            case BETWEEN -> getCriteria(part, property, v1, convertIfNecessary(parameters.next()), parameters,
                FilterOperation.BETWEEN);
            case LIKE, REGEX -> getCriteria(part, property, v1, null, parameters, FilterOperation.LIKE);
            case STARTING_WITH -> getCriteria(part, property, v1, null, parameters, FilterOperation.STARTS_WITH);
            case ENDING_WITH -> getCriteria(part, property, v1, null, parameters, FilterOperation.ENDS_WITH);
            case CONTAINING -> getCriteria(part, property, v1, null, parameters, FilterOperation.CONTAINING);
            case NOT_CONTAINING -> getCriteria(part, property, v1, null, parameters, FilterOperation.NOT_CONTAINING);
            case WITHIN -> {
                v1 = Value.get(String.format("{ \"type\": \"AeroCircle\", \"coordinates\": [[%.8f, %.8f], %f] }",
                    v1, parameters.next(), parameters.next()));
                yield getCriteria(part, property, v1, parameters.next(), parameters, FilterOperation.GEO_WITHIN);
            }
            case SIMPLE_PROPERTY -> getCriteria(part, property, v1, null, parameters, FilterOperation.EQ);
            case NEGATING_SIMPLE_PROPERTY -> getCriteria(part, property, v1, null, parameters, FilterOperation.NOTEQ);
            case IN -> getCriteria(part, property, v1, null, parameters, FilterOperation.IN);
            case NOT_IN -> getCriteria(part, property, v1, null, parameters, FilterOperation.NOT_IN);
            case TRUE -> getCriteria(part, property, true, null, parameters, FilterOperation.EQ);
            case FALSE -> getCriteria(part, property, false, null, parameters, FilterOperation.EQ);
            case EXISTS, IS_NOT_NULL -> getCriteria(part, property, null, null, parameters, IS_NOT_NULL);
            case IS_NULL -> getCriteria(part, property, null, null, parameters, IS_NULL);
            default -> throw new IllegalArgumentException("Unsupported keyword '" + part.getType() + "'");
        };
    }

    private Object convertIfNecessary(Object obj) {
        if (obj == null || obj instanceof AerospikeMapCriteria) {
            return obj;
        }

        // converting if necessary (e.g., Date to Long so that proper filter expression or sIndex filter can be built)
        final Object value = obj;
        TypeInformation<?> valueType = TypeInformation.of(value.getClass());
        return converter.toWritableValue(value, valueType);
    }

    public AerospikeCriteria getCriteria(Part part, AerospikePersistentProperty property, Object value1, Object value2,
                                         Iterator<?> parameters, FilterOperation op) {
        String fieldName = getFieldName(part.getProperty().getSegment(), property);
        Qualifier qualifier;

        if (property.isIdProperty()) {
            qualifier = processId(value1);
        } else if (property.isCollectionLike()) {
            qualifier = processCollection(part, value1, value2, parameters, op, fieldName);
        } else if (property.isMap()) {
            qualifier = processMap(part, value1, value2, parameters, op, fieldName);
        } else { // if it is neither a collection nor a map
            qualifier = processOther(part, value1, value2, property, op, fieldName);
        }

        return new AerospikeCriteria(qualifier);
    }

    private Qualifier processId(Object value1) {
        Qualifier qualifier;
        if (value1 instanceof Collection<?>) {
            // currently id can only be a String
            List<String> ids = ((Collection<?>) value1).stream().map(String::valueOf).toList();
            qualifier = idIn(ids.toArray(String[]::new));
        } else {
            qualifier = idEquals((String) value1);
        }
        return qualifier;
    }

    private Qualifier processCollection(Part part, Object value1, Object value2, Iterator<?> parameters,
                                        FilterOperation op, String fieldName) {
        Qualifier.QualifierBuilder qb = Qualifier.builder();
        List<Object> params = new ArrayList<>();
        parameters.forEachRemaining(params::add);

        if (!params.isEmpty()) {
            Object nextParam = params.get(params.size() - 1);
            if (op == FilterOperation.CONTAINING && !(nextParam instanceof AerospikeMapCriteria)) {
                op = LIST_VAL_CONTAINING;
                params.add(0, value1); // value1 stores the first parameter
                return qualifierAndConcatenated(params, qb, part, fieldName, op, null);
            }
        } else if (!(value1 instanceof Collection<?>)) {
            op = getCorrespondingListFilterOperationOrFail(op);
        }

        return setQualifier(qb, fieldName, op, part, value1, value2, null, null);
    }

    private Qualifier processMap(Part part, Object value1, Object value2, Iterator<?> parameters, FilterOperation op,
                                 String fieldName) {
        List<Object> params = new ArrayList<>();
        parameters.forEachRemaining(params::add);
        Qualifier qualifier = null;

        // the first parameter is value1, params have parameters starting from the second
        if (params.size() == 1) {
            qualifier = processMap2Params(part, value1, value2, params, op, fieldName);
        } else if (params.isEmpty()) {
            if (op != FilterOperation.BETWEEN) { // if not map in range (2 maps as parameters)
                // VALUE2 contains key (field name)
                qualifier = setQualifier(Qualifier.builder(), fieldName, op, part, value1, Value.get(fieldName),
                    null, null);
            }
        } else {
            qualifier = processMapMultipleParams(part, value1, value2, params, op, fieldName);
        }

        return qualifier;
    }

    private Qualifier processMap2Params(Part part, Object value1, Object value2, List<Object> params,
                                        FilterOperation op, String fieldName) {
        Qualifier qualifier;
        Object nextParam = convertIfNecessary(params.get(0)); // nextParam is de facto the second

        if (op == FilterOperation.CONTAINING) {
            qualifier = processMapContaining(nextParam, part, value1, fieldName, MAP_KEYS_CONTAIN, MAP_VALUES_CONTAIN,
                MAP_VAL_EQ_BY_KEY);
        } else if (op == FilterOperation.NOT_CONTAINING) {
            qualifier = processMapContaining(nextParam, part, value1, fieldName, MAP_KEYS_NOT_CONTAIN,
                MAP_VALUES_NOT_CONTAIN,
                MAP_VAL_NOTEQ_BY_KEY);
        } else {
            qualifier = processMapBetween(part, value1, value2, op, fieldName, nextParam);
        }

        return qualifier;
    }

    private Qualifier processMapContaining(Object nextParam, Part part, Object value1, String fieldName,
                                           FilterOperation keysOp, FilterOperation valuesOp, FilterOperation byKeyOp) {
        FilterOperation op;
        String dotPath = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (nextParam instanceof AerospikeMapCriteria onMap) {
            switch (onMap) {
                case KEY -> op = keysOp;
                case VALUE -> op = valuesOp;
                default -> throw new UnsupportedOperationException("Unsupported parameter: " + onMap);
            }
        } else {
            op = byKeyOp;
            dotPath = part.getProperty().toDotPath() + "." + Value.get(value1);
            setQbValuesForMapByKey(qb, value1, nextParam);
        }

        return setQualifier(qb, fieldName, op, part, value1, null, null, dotPath);
    }

    private Qualifier processMapBetween(Part part, Object value1, Object value2, FilterOperation op, String fieldName
        , Object nextParam) {
        Qualifier.QualifierBuilder qb = Qualifier.builder();
        String dotPath = part.getProperty().toDotPath() + "." + Value.get(value1);

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

        return setQualifier(qb, fieldName, op, part, value1, value2, null, dotPath);
    }

    private Qualifier processMapMultipleParams(Part part, Object value1, Object value2, List<Object> params,
                                               FilterOperation op, String fieldName) {
        if (op == FilterOperation.CONTAINING) {
            return processMapMultipleParamsContaining(part, value1, value2, params, op, fieldName);
        } else {
            String paramsString = params.stream().map(Object::toString).collect(Collectors.joining(", "));
            throw new IllegalArgumentException(
                "Expected not more than 2 arguments (propertyType: Map, filterOperation: " + op + "), " +
                    " got " + (params.size() + 1) + " instead: '" + value1 + ", " + paramsString + "'");
        }
    }

    private Qualifier processMapMultipleParamsContaining(Part part, Object value1, Object value2, List<Object> params,
                                                         FilterOperation op, String fieldName) {
        String dotPath = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (params.get(params.size() - 1) instanceof AerospikeMapCriteria mapCriteria) {
            switch (mapCriteria) {
                case KEY -> op = MAP_KEYS_CONTAIN;
                case VALUE -> op = MAP_VALUES_CONTAIN;
                case VALUE_CONTAINING -> op = MAP_VAL_CONTAINING_BY_KEY;
            }
            params = params.stream().limit(params.size() - 1L).collect(Collectors.toList());
        } else {
            op = MAP_VAL_EQ_BY_KEY;
            dotPath = part.getProperty().toDotPath() + "." + Value.get(value1);
        }

        params.add(0, value1); // value1 stores the first parameter
        if (op == MAP_VAL_CONTAINING_BY_KEY || op == MAP_VAL_EQ_BY_KEY) {
            return processMapMultipleParamsContainingPerSize(params, qb, part, value1, value2, fieldName, op, dotPath);
        } else {
            return qualifierAndConcatenated(params, qb, part, fieldName, op, dotPath);
        }
    }

    private Qualifier processMapMultipleParamsContainingPerSize(List<Object> params, Qualifier.QualifierBuilder qb,
                                                                Part part, Object value1, Object value2,
                                                                String fieldName, FilterOperation op, String dotPath) {
        if (params.size() > 2) {
            if ((params.size() & 1) != 0) { // if params.size() is an odd number
                throw new IllegalArgumentException("FindByMapContaining: expected either one, two " +
                    "or even number of key/value arguments, instead got " + params.size());
            }
            return qualifierAndConcatenated(params, qb, part, fieldName, op, dotPath, true);
        } else if (params.size() == 2) {
            setQbValuesForMapByKey(qb, params.get(0), params.get(1));
            return setQualifier(qb, fieldName, op, part, value1, value2, null, null);
        } else {
            throw new UnsupportedOperationException("Unsupported combination of operation " + op + " and " +
                "parameters with size of + " + params.size());
        }
    }

    private Qualifier processOther(Part part, Object value1, Object value2, AerospikePersistentProperty property,
                                   FilterOperation op, String fieldName) {
        String dotPath = null;
        Object value3 = null;
        Qualifier.QualifierBuilder qb = Qualifier.builder();

        if (part.getProperty().hasNext()) { // if it is a POJO field (a simple field or an inner POJO)
            if (op == FilterOperation.BETWEEN) {
                value3 = Value.get(value2); // contains upper limit
            } else if (op == IS_NOT_NULL || op == IS_NULL) {
                value1 = Value.get(property.getFieldName()); // contains key (field name)
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

        return setQualifier(qb, fieldName, op, part, value1, value2, value3, dotPath);
    }

    private String getFieldName(String segmentName, AerospikePersistentProperty property) {
        org.springframework.data.aerospike.mapping.Field annotation =
            property.findAnnotation(org.springframework.data.aerospike.mapping.Field.class);

        if (annotation != null && StringUtils.hasText(annotation.value())) {
            return annotation.value();
        }

        if (!StringUtils.hasText(segmentName)) {
            throw new IllegalStateException("Segment name is null or empty");
        }

        return segmentName;
    }

    private Qualifier qualifierAndConcatenated(List<Object> params, Qualifier.QualifierBuilder qb,
                                               Part part, String fieldName, FilterOperation op,
                                               String dotPath) {
        return qualifierAndConcatenated(params, qb, part, fieldName, op, dotPath, false);
    }

    private Qualifier qualifierAndConcatenated(List<Object> params, Qualifier.QualifierBuilder qb,
                                               Part part, String fieldName, FilterOperation op,
                                               String dotPath, boolean containingMapKeyValuePairs) {
        Qualifier[] qualifiers;
        if (containingMapKeyValuePairs) {
            qualifiers = new Qualifier[params.size() / 2]; // keys/values qty must be even
            for (int i = 0, j = 0; i < params.size(); i += 2, j++) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i + 1));
                qualifiers[j] = setQualifier(qb, fieldName, op, part, params.get(i),
                    null, null, dotPath);
            }

            return Qualifier.and(qualifiers);
        } else {
            qualifiers = new Qualifier[params.size()];
            for (int i = 0; i < params.size(); i++) {
                setQbValuesForMapByKey(qb, params.get(i), params.get(i));
                qualifiers[i] = setQualifier(qb, fieldName, op, part, params.get(i),
                    null, null, dotPath);
            }
        }

        return Qualifier.and(qualifiers);
    }

    private Qualifier setQualifier(Qualifier.QualifierBuilder qb, String fieldName, FilterOperation op, Part part,
                                   Object value1, Object value2, Object value3, String dotPath) {
        qb.setField(fieldName)
            .setFilterOperation(op)
            .setIgnoreCase(ignoreCaseToBoolean(part))
            .setConverter(converter);
        setNotNullQbValues(qb, value1, value2, value3, dotPath);
        return qb.build();
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

        return new AerospikeCriteria(Qualifier.and(base, create(part, property,
            iterator)));
    }

    @Override
    protected AerospikeCriteria or(AerospikeCriteria base, AerospikeCriteria criteria) {
        return new AerospikeCriteria(Qualifier.or(base, criteria));
    }

    @Override
    protected Query complete(AerospikeCriteria criteria, Sort sort) {
        Query query = criteria == null ? null : new Query(criteria).with(sort);

        if (LOG.isDebugEnabled() && criteria != null) {
            logQualifierDetails(criteria);
        }

        return query;
    }

    private void logQualifierDetails(Qualifier criteria) {
        Qualifier[] qualifiers = criteria.getQualifiers();
        if (qualifiers != null && qualifiers.length > 0) {
            Arrays.stream(qualifiers).forEach(this::logQualifierDetails);
        }

        String field = (StringUtils.hasLength(criteria.getField()) ? criteria.getField() : "");
        String operation = (StringUtils.hasLength(criteria.getOperation().toString()) ?
            criteria.getOperation().toString() : "N/A");
        String value1 = (criteria.getValue1() != null && !criteria.getValue1().toString().isEmpty() ?
            criteria.getValue1().toString() : "");
        String value2 = (criteria.getValue2() != null && !criteria.getValue2().toString().isEmpty() ?
            criteria.getValue2().toString() : "");

        LOG.debug("Created query: {} {} {} {}", field, operation, value1, value2);
    }

    private boolean ignoreCaseToBoolean(Part part) {
        return switch (part.shouldIgnoreCase()) {
            case WHEN_POSSIBLE -> part.getProperty().getType() == String.class;
            case ALWAYS -> true;
            default -> false;
        };
    }
}
