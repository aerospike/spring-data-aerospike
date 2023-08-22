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

import org.springframework.data.aerospike.core.ReactiveAerospikeInternalOperations;
import org.springframework.data.aerospike.core.ReactiveAerospikeOperations;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.repository.query.ParametersParameterAccessor;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import reactor.core.publisher.Flux;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikePartTreeQuery extends BaseAerospikePartTreeQuery {

    private final ReactiveAerospikeOperations operations;
    private final ReactiveAerospikeInternalOperations internalOperations;

    public ReactiveAerospikePartTreeQuery(QueryMethod queryMethod,
                                          QueryMethodEvaluationContextProvider evalContextProvider,
                                          ReactiveAerospikeTemplate aerospikeTemplate,
                                          Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        super(queryMethod, evalContextProvider, queryCreator);
        this.operations = aerospikeTemplate;
        this.internalOperations = aerospikeTemplate;
    }

    @Override
    public Object execute(Object[] parameters) {
        ParametersParameterAccessor accessor = new ParametersParameterAccessor(queryMethod.getParameters(), parameters);
        Query query = prepareQuery(parameters, accessor);
        Class<?> targetClass = getTargetClass(accessor);

        // queries that include id have their own processing flow
        if (parameters != null && parameters.length > 0) {
            AerospikeCriteria criteria = query.getAerospikeCriteria();
            Qualifier[] qualifiers = getQualifiers(criteria);
            if (isIdQuery(criteria)) {
                return runIdQuery(entityClass, targetClass, getIdValue(qualifiers));
            } else if (hasIdQualifier(criteria)) {
                return runIdQuery(entityClass, targetClass, getIdValue(getIdQualifier(qualifiers)),
                    excludeIdQualifier(qualifiers));
            }
        }

        return findByQuery(query, targetClass);
    }

    private Flux<?> findByQuery(Query query, Class<?> targetClass) {
        // Run query and map to different target class.
        if (targetClass != entityClass) {
            return operations.find(query, entityClass, targetClass);
        }
        // Run query and map to entity class type.
        return operations.find(query, entityClass);
    }

    protected Object findById(Object obj, Class<?> sourceClass, Class<?> targetClass, Qualifier... qualifiers) {
        return internalOperations.findByIdInternal(obj, sourceClass, targetClass, qualifiers);
    }

    protected Object findByIds(Iterable<?> iterable, Class<?> sourceClass, Class<?> targetClass,
                               Qualifier... qualifiers) {
        return internalOperations.findByIdsInternal(IterableConverter.toList(iterable), sourceClass, targetClass,
                qualifiers);
    }
}
