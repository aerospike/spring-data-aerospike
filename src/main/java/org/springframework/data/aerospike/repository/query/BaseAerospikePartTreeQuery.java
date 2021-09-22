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
import org.springframework.data.repository.query.*;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.util.ClassUtils;

import java.lang.reflect.Constructor;

/**
 * @author Peter Milne
 * @author Jean Mercier
 * @author Igor Ermolenko
 */
public abstract class BaseAerospikePartTreeQuery implements RepositoryQuery {

    private final QueryMethodEvaluationContextProvider evaluationContextProvider;
    private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;
    protected final QueryMethod queryMethod;

    public BaseAerospikePartTreeQuery(QueryMethod queryMethod,
                                      QueryMethodEvaluationContextProvider evalContextProvider,
                                      Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        this.queryMethod = queryMethod;
        this.evaluationContextProvider = evalContextProvider;
        this.queryCreator = queryCreator;
    }

    @Override
    public QueryMethod getQueryMethod() {
        return queryMethod;
    }

    protected Query prepareQuery(Object[] parameters, ParametersParameterAccessor accessor) {
        Query query = createQuery(accessor);

        AerospikeCriteria criteria = (AerospikeCriteria) query.getCriteria();
        Query q = new Query(criteria);

        if (accessor.getPageable().isPaged()) {
            q.setOffset(accessor.getPageable().getOffset());
            q.setRows(accessor.getPageable().getPageSize());
        } else {
            q.setOffset(-1);
            q.setRows(-1);
        }

        if (accessor.getSort().isSorted()) {
            q.setSort(accessor.getSort());
        } else {
            q.setSort(query.getSort());
        }

        if (q.getCriteria() instanceof SpelExpression) {
            EvaluationContext context = this.evaluationContextProvider.getEvaluationContext(queryMethod.getParameters(),
                    parameters);
            ((SpelExpression) q.getCriteria()).setEvaluationContext(context);
        }

        return q;
    }

    public Query createQuery(ParametersParameterAccessor accessor) {
        PartTree tree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());

        Constructor<? extends AbstractQueryCreator<?, ?>> constructor = ClassUtils
                .getConstructorIfAvailable(queryCreator, PartTree.class, ParameterAccessor.class);
        return (Query) BeanUtils.instantiateClass(constructor, tree, accessor).createQuery();
    }
}
