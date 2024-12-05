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
package org.springframework.data.aerospike.repository.support;

import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.aerospike.repository.query.ReactiveAerospikePartTreeQuery;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.util.Assert;
import reactor.util.annotation.Nullable;

import java.lang.reflect.Method;
import java.util.Optional;

import static org.springframework.data.querydsl.QuerydslUtils.QUERY_DSL_PRESENT;

/**
 * @author Igor Ermolenko
 */
public class ReactiveAerospikeRepositoryFactory extends ReactiveRepositoryFactorySupport {

    private static final Class<AerospikeQueryCreator> DEFAULT_QUERY_CREATOR = AerospikeQueryCreator.class;

    private final MappingContext<? extends AerospikePersistentEntity<?>, AerospikePersistentProperty> context;
    private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;
    private final ReactiveAerospikeTemplate aerospikeTemplate;

    public ReactiveAerospikeRepositoryFactory(ReactiveAerospikeTemplate template) {
        this(template, DEFAULT_QUERY_CREATOR);
    }

    @SuppressWarnings("unchecked")
    public ReactiveAerospikeRepositoryFactory(ReactiveAerospikeTemplate aerospikeTemplate,
                                              Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        Assert.notNull(aerospikeTemplate, "AerospikeTemplate must not be null!");
        Assert.notNull(queryCreator, "Query creator type must not be null!");
        this.queryCreator = queryCreator;
        this.aerospikeTemplate = aerospikeTemplate;
        this.context = (MappingContext<? extends AerospikePersistentEntity<?>, AerospikePersistentProperty>)
            aerospikeTemplate.getMappingContext();
    }

    /**
     * Returns whether the given repository interface requires a QueryDsl specific implementation to be chosen.
     *
     * @param repositoryInterface must not be {@literal null}.
     * @return An indication if the given repository requires a QueryDsl implementation.
     */
    private static boolean isQueryDslRepository(Class<?> repositoryInterface) {
        return QUERY_DSL_PRESENT && QuerydslPredicateExecutor.class.isAssignableFrom(repositoryInterface);
    }

    @SuppressWarnings({"unchecked", "NullableProblems"})
    @Override
    public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
        AerospikePersistentEntity<?> entity = context.getRequiredPersistentEntity(domainClass);
        return new PersistentEntityInformation<>((AerospikePersistentEntity<T>) entity);
    }

    @Override
    protected Object getTargetRepository(RepositoryInformation repositoryInformation) {
        EntityInformation<?, Object> entityInformation = getEntityInformation(repositoryInformation.getDomainType());
        return getTargetRepositoryViaReflection(repositoryInformation, entityInformation, aerospikeTemplate);
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return SimpleReactiveAerospikeRepository.class;
    }

    @Override
    protected Optional<QueryLookupStrategy> getQueryLookupStrategy(
        Key key,
        QueryMethodEvaluationContextProvider evaluationContextProvider)
    {
        return Optional.of(
            new ReactiveAerospikeQueryLookupStrategy(key, evaluationContextProvider, aerospikeTemplate, queryCreator));
    }

    /**
     * @author Christoph Strobl
     * @author Oliver Gierke
     */
    private static class ReactiveAerospikeQueryLookupStrategy implements QueryLookupStrategy {

        private final QueryMethodEvaluationContextProvider evaluationContextProvider;
        private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;
        private final ReactiveAerospikeTemplate aerospikeTemplate;

        /**
         * Creates a new {@link ReactiveAerospikeQueryLookupStrategy} for the given {@link Key},
         * {@link QueryMethodEvaluationContextProvider} and query creator type.
         * <p>
         *
         * @param key                       Currently unused, same behaviour in the built-in spring's
         *                                  KeyValueQueryLookupStrategy implementation.
         * @param evaluationContextProvider must not be {@literal null}.
         * @param aerospikeTemplate         must not be {@literal null}.
         * @param queryCreator              must not be {@literal null}.
         */
        public ReactiveAerospikeQueryLookupStrategy(@Nullable Key key,
                                                    QueryMethodEvaluationContextProvider evaluationContextProvider,
                                                    ReactiveAerospikeTemplate aerospikeTemplate,
                                                    Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
            Assert.notNull(evaluationContextProvider, "QueryMethodEvaluationContextProvider must not be null!");
            Assert.notNull(aerospikeTemplate, "AerospikeTemplate must not be null!");
            Assert.notNull(queryCreator, "Query creator type must not be null!");
            this.evaluationContextProvider = evaluationContextProvider;
            this.aerospikeTemplate = aerospikeTemplate;
            this.queryCreator = queryCreator;
        }

        @Override
        public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata,
                                            ProjectionFactory projectionFactory,
                                            NamedQueries namedQueries) {
            QueryMethod queryMethod = new QueryMethod(method, metadata, projectionFactory);
            return new ReactiveAerospikePartTreeQuery(queryMethod, evaluationContextProvider, this.aerospikeTemplate,
                this.queryCreator);
        }
    }
}
