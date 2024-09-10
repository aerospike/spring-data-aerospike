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
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.index.AerospikeIndexResolver;
import org.springframework.data.aerospike.index.ReactiveAerospikePersistenceEntityIndexCreator;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.FilterExpressionsBuilder;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.cache.IndexesCacheUpdater;
import org.springframework.data.aerospike.query.cache.InternalIndexOperations;
import org.springframework.data.aerospike.query.cache.ReactorIndexRefresher;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;

/**
 * Configuration with beans needed for reactive flow
 *
 * @author Igor Ermolenko
 */
@Slf4j
@Configuration
public abstract class AbstractReactiveAerospikeDataConfiguration extends AerospikeDataConfigurationSupport {

    @Bean(name = "reactiveAerospikeTemplate")
    public ReactiveAerospikeTemplate reactiveAerospikeTemplate(MappingAerospikeConverter mappingAerospikeConverter,
                                                               AerospikeMappingContext aerospikeMappingContext,
                                                               AerospikeExceptionTranslator aerospikeExceptionTranslator,
                                                               IAerospikeReactorClient aerospikeReactorClient,
                                                               ReactorQueryEngine reactorQueryEngine,
                                                               ReactorIndexRefresher reactorIndexRefresher,
                                                               ServerVersionSupport serverVersionSupport,
                                                               AerospikeSettings settings) {
        return new ReactiveAerospikeTemplate(aerospikeReactorClient, settings.getConnectionSettings().getNamespace(),
            mappingAerospikeConverter, aerospikeMappingContext, aerospikeExceptionTranslator,
            reactorQueryEngine, reactorIndexRefresher, serverVersionSupport);
    }

    @Bean(name = "reactiveAerospikeQueryEngine")
    public ReactorQueryEngine reactorQueryEngine(IAerospikeReactorClient aerospikeReactorClient,
                                                 StatementBuilder statementBuilder,
                                                 FilterExpressionsBuilder filterExpressionsBuilder,
                                                 AerospikeSettings settings) {
        ReactorQueryEngine queryEngine = new ReactorQueryEngine(aerospikeReactorClient, statementBuilder,
            filterExpressionsBuilder, settings.getDataSettings());
        boolean scansEnabled = settings.getDataSettings().isScansEnabled();
        queryEngine.setScansEnabled(scansEnabled);
        log.info("AerospikeDataSettings.scansEnabled: {}", scansEnabled);
        long queryMaxRecords = settings.getDataSettings().getQueryMaxRecords();
        log.info("AerospikeDataSettings.queryMaxRecords: {}", queryMaxRecords);
        queryEngine.setQueryMaxRecords(queryMaxRecords);
        if (!settings.getDataSettings().isWriteSortedMaps()) {
            log.info("AerospikeDataSettings.writeSortedMaps is set to false, " +
                "Maps and POJOs will be written as unsorted Maps (degrades performance of Map-related operations," +
                " does not allow comparing Maps)");
        }
        return queryEngine;
    }

    @Bean(name = "reactiveAerospikeIndexRefresher")
    public ReactorIndexRefresher reactorIndexRefresher(IAerospikeReactorClient aerospikeReactorClient,
                                                       IndexesCacheUpdater indexesCacheUpdater,
                                                       ServerVersionSupport serverVersionSupport) {
        ReactorIndexRefresher refresher = new ReactorIndexRefresher(aerospikeReactorClient,
            aerospikeReactorClient.getInfoPolicyDefault(),
            new InternalIndexOperations(new IndexInfoParser()), indexesCacheUpdater, serverVersionSupport);
        refresher.refreshIndexes().block();
        return refresher;
    }

    @Bean(name = "aerospikeReactorClient")
    public IAerospikeReactorClient aerospikeReactorClient(IAerospikeClient aerospikeClient) {
        return new AerospikeReactorClient(aerospikeClient);
    }

    @Bean(name = "aerospikeReactorClient1")
    public IAerospikeReactorClient aerospikeReactorClient1(IAerospikeClient aerospikeClient) {
        return new AerospikeReactorClient(aerospikeClient);
    }

    @Override
    protected ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = super.getClientPolicy(); // applying default values first
        // set particular clientPolicy fields if needed
        return clientPolicy;
    }

    @Bean
    public ReactiveAerospikePersistenceEntityIndexCreator aerospikePersistenceEntityIndexCreator(
        ObjectProvider<AerospikeMappingContext> aerospikeMappingContext,
        AerospikeIndexResolver aerospikeIndexResolver,
        ObjectProvider<ReactiveAerospikeTemplate> template, AerospikeSettings settings) {
        boolean indexesOnStartup = settings.getDataSettings().isCreateIndexesOnStartup();
        log.info("AerospikeDataSettings.indexesOnStartup: {}", indexesOnStartup);
        return new ReactiveAerospikePersistenceEntityIndexCreator(aerospikeMappingContext,
            indexesOnStartup, aerospikeIndexResolver, template);
    }
}
