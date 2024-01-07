/*
 * Copyright 2017 the original author or authors.
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
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.index.AerospikeIndexResolver;
import org.springframework.data.aerospike.index.AerospikePersistenceEntityIndexCreator;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.FilterExpressionsBuilder;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.query.cache.IndexesCacheUpdater;
import org.springframework.data.aerospike.query.cache.InternalIndexOperations;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;

@Slf4j
@Configuration
public abstract class AbstractAerospikeDataConfiguration extends AerospikeDataConfigurationSupport {

    @Bean(name = "aerospikeTemplate")
    public AerospikeTemplate aerospikeTemplate(IAerospikeClient aerospikeClient,
                                               MappingAerospikeConverter mappingAerospikeConverter,
                                               AerospikeMappingContext aerospikeMappingContext,
                                               AerospikeExceptionTranslator aerospikeExceptionTranslator,
                                               QueryEngine queryEngine, IndexRefresher indexRefresher,
                                               ServerVersionSupport serverVersionSupport,
                                               @Qualifier("aerospikeSettings") AerospikeSettings settings) {
        return new AerospikeTemplate(aerospikeClient, settings.getNamespace(),
            mappingAerospikeConverter, aerospikeMappingContext, aerospikeExceptionTranslator, queryEngine,
            indexRefresher, serverVersionSupport);
    }

    @Bean(name = "aerospikeQueryEngine")
    public QueryEngine queryEngine(IAerospikeClient aerospikeClient,
                                   StatementBuilder statementBuilder,
                                   FilterExpressionsBuilder filterExpressionsBuilder,
                                   @Qualifier("readAerospikeDataSettings") AerospikeDataSettings settings) {
        QueryEngine queryEngine = new QueryEngine(aerospikeClient, statementBuilder, filterExpressionsBuilder);
        boolean scansEnabled = settings.isScansEnabled();
        log.debug("AerospikeDataSettings.scansEnabled: {}", scansEnabled);
        queryEngine.setScansEnabled(scansEnabled);
        long queryMaxRecords = settings.getQueryMaxRecords();
        log.debug("AerospikeDataSettings.queryMaxRecords: {}", queryMaxRecords);
        queryEngine.setQueryMaxRecords(queryMaxRecords);
        return queryEngine;
    }

    @Bean
    public AerospikePersistenceEntityIndexCreator aerospikePersistenceEntityIndexCreator(
        ObjectProvider<AerospikeMappingContext> aerospikeMappingContext,
        AerospikeIndexResolver aerospikeIndexResolver,
        ObjectProvider<AerospikeTemplate> template,
        @Qualifier("aerospikeDataSettings") AerospikeDataSettings settings) {
        boolean indexesOnStartup = settings.isCreateIndexesOnStartup();
        log.debug("AerospikeDataSettings.indexesOnStartup: {}", indexesOnStartup);
        return new AerospikePersistenceEntityIndexCreator(aerospikeMappingContext, indexesOnStartup,
            aerospikeIndexResolver, template);
    }

    @Bean(name = "aerospikeIndexRefresher")
    public IndexRefresher indexRefresher(IAerospikeClient aerospikeClient, IndexesCacheUpdater indexesCacheUpdater,
                                         ServerVersionSupport serverVersionSupport, @Qualifier("aerospikeDataSettings")
                                         AerospikeDataSettings settings) {
        IndexRefresher refresher = new IndexRefresher(aerospikeClient, aerospikeClient.getInfoPolicyDefault(),
            new InternalIndexOperations(new IndexInfoParser()), indexesCacheUpdater, serverVersionSupport);
        refresher.refreshIndexes();
        int refreshFrequency = settings.getIndexCacheRefreshSeconds();
        processCacheRefreshFrequency(refreshFrequency, refresher);
        log.debug("AerospikeDataSettings.indexCacheRefreshSeconds: {}", refreshFrequency);
        return refresher;
    }

    private void processCacheRefreshFrequency(int indexCacheRefreshSeconds, IndexRefresher indexRefresher) {
        if (indexCacheRefreshSeconds <= 0) {
            log.info("Periodic index cache refreshing is not scheduled, interval ({}) is <= 0",
                indexCacheRefreshSeconds);
        } else {
            indexRefresher.scheduleRefreshIndexes(indexCacheRefreshSeconds);
        }
    }
}
