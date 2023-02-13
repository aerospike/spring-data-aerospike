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
package org.springframework.data.aerospike.repository.support;

import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.repository.query.AerospikePartTreeQuery;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.keyvalue.core.KeyValueOperations;
import org.springframework.data.keyvalue.repository.support.QuerydslKeyValueRepository;
import org.springframework.data.keyvalue.repository.support.SimpleKeyValueRepository;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.querydsl.QuerydslPredicateExecutor;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
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
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeRepositoryFactory extends RepositoryFactorySupport {

    private static final Class<AerospikeQueryCreator> DEFAULT_QUERY_CREATOR = AerospikeQueryCreator.class;

    private final AerospikeOperations aerospikeOperations;
    private final MappingContext<? extends AerospikePersistentEntity<?>, AerospikePersistentProperty> context;
    private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;

    public AerospikeRepositoryFactory(AerospikeOperations aerospikeOperations) {
        this(aerospikeOperations, DEFAULT_QUERY_CREATOR);
    }

    @SuppressWarnings("unchecked")
    public AerospikeRepositoryFactory(AerospikeOperations aerospikeOperations,
                                      Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        Assert.notNull(aerospikeOperations, "AerospikeOperations must not be null!");
        Assert.notNull(queryCreator, "Query creator type must not be null!");
        this.queryCreator = queryCreator;
        this.aerospikeOperations = aerospikeOperations;
        this.context = (MappingContext<? extends AerospikePersistentEntity<?>, AerospikePersistentProperty>)
            aerospikeOperations.getMappingContext();
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
        return super.getTargetRepositoryViaReflection(repositoryInformation, entityInformation, aerospikeOperations);
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return isQueryDslRepository(metadata.getRepositoryInterface()) ? QuerydslKeyValueRepository.class
            : SimpleKeyValueRepository.class;
    }

    @Override
    protected Optional<QueryLookupStrategy> getQueryLookupStrategy(
        Key key,
        QueryMethodEvaluationContextProvider evaluationContextProvider) {
        return Optional.of(new AerospikeQueryLookupStrategy(key, evaluationContextProvider, this.aerospikeOperations,
            this.queryCreator));
    }

    /**
     * @author Christoph Strobl
     * @author Oliver Gierke
     */
    private static class AerospikeQueryLookupStrategy implements QueryLookupStrategy {

        private final QueryMethodEvaluationContextProvider evaluationContextProvider;
        private final AerospikeOperations aerospikeOperations;
        private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;

        /**
         * Creates a new {@link AerospikeQueryLookupStrategy} for the given {@link Key},
         * {@link QueryMethodEvaluationContextProvider}, {@link KeyValueOperations} and query creator type.
         * <p>
         * TODO: Key is not considered. Should it?
         *
         * @param key                       Currently unused, same behaviour in the built-in spring's
         *                                  KeyValueQueryLookupStrategy implementation.
         * @param evaluationContextProvider must not be {@literal null}.
         * @param aerospikeOperations       must not be {@literal null}.
         * @param queryCreator              must not be {@literal null}.
         */
        public AerospikeQueryLookupStrategy(@Nullable Key key,
                                            QueryMethodEvaluationContextProvider evaluationContextProvider,
                                            AerospikeOperations aerospikeOperations, Class<?
            extends AbstractQueryCreator<?, ?>> queryCreator) {
            Assert.notNull(evaluationContextProvider, "QueryMethodEvaluationContextProvider must not be null!");
            Assert.notNull(aerospikeOperations, "AerospikeOperations must not be null!");
            Assert.notNull(queryCreator, "Query creator type must not be null!");
            this.evaluationContextProvider = evaluationContextProvider;
            this.aerospikeOperations = aerospikeOperations;
            this.queryCreator = queryCreator;
        }

        @Override
        public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata,
                                            ProjectionFactory projectionFactory,
                                            NamedQueries namedQueries) {
            QueryMethod queryMethod = new QueryMethod(method, metadata, projectionFactory);
            return new AerospikePartTreeQuery(queryMethod, evaluationContextProvider, this.aerospikeOperations,
                this.queryCreator);
        }
    }
}
