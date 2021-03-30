/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.config;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.index.ReactiveAerospikePersistenceEntityIndexCreator;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.cache.IndexesCacheUpdater;
import org.springframework.data.aerospike.query.cache.InternalIndexOperations;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;

/**
 * Configuration with beans needed for reactive stuff
 *
 * @author Igor Ermolenko
 */
@Configuration
public abstract class AbstractReactiveAerospikeDataConfiguration extends AerospikeDataConfigurationSupport {

    @Bean(name = "reactiveAerospikeTemplate")
    public ReactiveAerospikeTemplate reactiveAerospikeTemplate(MappingAerospikeConverter mappingAerospikeConverter,
                                                               AerospikeMappingContext aerospikeMappingContext,
                                                               AerospikeExceptionTranslator aerospikeExceptionTranslator,
                                                               AerospikeReactorClient aerospikeReactorClient,
                                                               ReactorQueryEngine reactorQueryEngine, ReactorIndexRefresher reactorIndexRefresher) {
        return new ReactiveAerospikeTemplate(aerospikeReactorClient, nameSpace(), mappingAerospikeConverter, aerospikeMappingContext,
                aerospikeExceptionTranslator, reactorQueryEngine, reactorIndexRefresher);
    }

    @Bean(name = "reactiveAerospikeQueryEngine")
    public ReactorQueryEngine reactorQueryEngine(AerospikeReactorClient aerospikeReactorClient,
                                                 StatementBuilder statementBuilder) {
        ReactorQueryEngine queryEngine = new ReactorQueryEngine(aerospikeReactorClient, statementBuilder, aerospikeReactorClient.getQueryPolicyDefault());
        queryEngine.setScansEnabled(aerospikeDataSettings().isScansEnabled());
        return queryEngine;
    }

    @Bean(name = "reactiveAerospikeIndexRefresher")
    public ReactorIndexRefresher reactorIndexRefresher(AerospikeReactorClient aerospikeReactorClient, IndexesCacheUpdater indexesCacheUpdater) {
        ReactorIndexRefresher refresher = new ReactorIndexRefresher(aerospikeReactorClient, aerospikeReactorClient.getInfoPolicyDefault(),
                new InternalIndexOperations(new IndexInfoParser()), indexesCacheUpdater);
        refresher.refreshIndexes().block();
        return refresher;
    }

    @Bean(name = "aerospikeReactorClient")
    public AerospikeReactorClient aerospikeReactorClient(IAerospikeClient aerospikeClient, EventLoops eventLoops) {
        return new AerospikeReactorClient(aerospikeClient, eventLoops);
    }

    @Bean
    protected abstract EventLoops eventLoops();

    @Override
    protected ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = super.getClientPolicy();
        clientPolicy.eventLoops = eventLoops();
        return clientPolicy;
    }

    @Bean
    public ReactiveAerospikePersistenceEntityIndexCreator aerospikePersistenceEntityIndexCreator(
            AerospikeMappingContext aerospikeMappingContext,
            @Lazy ReactiveAerospikeTemplate template) {
        return new ReactiveAerospikePersistenceEntityIndexCreator(aerospikeMappingContext, template);
    }
}
