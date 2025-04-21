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

import org.springframework.data.aerospike.repository.query.AerospikeQueryMethod;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.repository.query.AerospikePartTreeQuery;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
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
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.lang.reflect.Method;
import java.util.Optional;

import static org.springframework.data.querydsl.QuerydslUtils.QUERY_DSL_PRESENT;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class AerospikeRepositoryFactory extends RepositoryFactorySupport {

    private static final Class<AerospikeQueryCreator> DEFAULT_QUERY_CREATOR = AerospikeQueryCreator.class;

    private final AerospikeTemplate template;
    private final MappingContext<? extends AerospikePersistentEntity<?>, AerospikePersistentProperty> context;
    private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;

    public AerospikeRepositoryFactory(AerospikeTemplate aerospikeTemplate) {
        this(aerospikeTemplate, DEFAULT_QUERY_CREATOR);
    }

    @SuppressWarnings("unchecked")
    public AerospikeRepositoryFactory(AerospikeTemplate aerospikeTemplate,
                                      Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
        Assert.notNull(aerospikeTemplate, "AerospikeTemplate must not be null!");
        Assert.notNull(queryCreator, "Query creator type must not be null!");
        this.queryCreator = queryCreator;
        this.template = aerospikeTemplate;
        this.context = (MappingContext<? extends AerospikePersistentEntity<?>, AerospikePersistentProperty>)
            template.getMappingContext();
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
        return super.getTargetRepositoryViaReflection(repositoryInformation, entityInformation, template);
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return SimpleAerospikeRepository.class;
    }

    @Override
    protected Optional<QueryLookupStrategy> getQueryLookupStrategy(@Nullable Key key,
                                                                   ValueExpressionDelegate valueExpressionDelegate) {
        return Optional.of(
            new AerospikeQueryLookupStrategy(valueExpressionDelegate.getEvaluationContextAccessor(),
            this.template,
            this.queryCreator)
        );
    }

    /**
     * @author Christoph Strobl
     * @author Oliver Gierke
     */
    private static class AerospikeQueryLookupStrategy implements QueryLookupStrategy {

        private final QueryMethodValueEvaluationContextAccessor evaluationContextAccessor;
        private final AerospikeTemplate aerospikeTemplate;
        private final Class<? extends AbstractQueryCreator<?, ?>> queryCreator;

        /**
         * Creates a new {@link AerospikeQueryLookupStrategy} for the given {@link Key},
         * {@link QueryMethodValueEvaluationContextAccessor} and query creator type.
         * <p>
         *
         * @param evaluationContextAccessor must not be {@literal null}.
         * @param aerospikeTemplate         must not be {@literal null}.
         * @param queryCreator              must not be {@literal null}.
         */
        public AerospikeQueryLookupStrategy(QueryMethodValueEvaluationContextAccessor evaluationContextAccessor,
                                            AerospikeTemplate aerospikeTemplate,
                                            Class<? extends AbstractQueryCreator<?, ?>> queryCreator) {
            Assert.notNull(evaluationContextAccessor, "QueryMethodEvaluationContextAccessor must not be null!");
            Assert.notNull(aerospikeTemplate, "AerospikeTemplate must not be null!");
            Assert.notNull(queryCreator, "Query creator type must not be null!");
            this.evaluationContextAccessor = evaluationContextAccessor;
            this.aerospikeTemplate = aerospikeTemplate;
            this.queryCreator = queryCreator;
        }

        @Override
        public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata,
                                            ProjectionFactory projectionFactory,
                                            NamedQueries namedQueries) {
            AerospikeQueryMethod queryMethod = new AerospikeQueryMethod(method, metadata, projectionFactory);
            return new AerospikePartTreeQuery(queryMethod, evaluationContextAccessor, this.aerospikeTemplate,
                this.queryCreator);
        }
    }
}
