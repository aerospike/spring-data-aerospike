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
import com.aerospike.dsl.api.DSLParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
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
import org.springframework.data.aerospike.query.QueryContextBuilder;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.query.cache.IndexesCacheHolder;
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
                                               IndexesCacheHolder indexCacheHolder,
                                               ServerVersionSupport serverVersionSupport,
                                               AerospikeSettings settings, DSLParser dslParser) {
        return new AerospikeTemplate(aerospikeClient, settings.getDataSettings().getNamespace(),
            mappingAerospikeConverter, aerospikeMappingContext, aerospikeExceptionTranslator, queryEngine,
            indexRefresher, indexCacheHolder, serverVersionSupport, dslParser);
    }

    @Bean(name = "aerospikeQueryEngine")
    public QueryEngine queryEngine(IAerospikeClient aerospikeClient, QueryContextBuilder queryContextBuilder,
                                   IndexesCacheHolder indexCacheHolder,
                                   FilterExpressionsBuilder filterExpressionsBuilder, AerospikeSettings settings,
                                   DSLParser dslParser) {
        QueryEngine queryEngine = new QueryEngine(aerospikeClient, queryContextBuilder, filterExpressionsBuilder,
            settings.getDataSettings(), indexCacheHolder, dslParser);
        boolean scansEnabled = settings.getDataSettings().isScansEnabled();
        log.info("AerospikeDataSettings.scansEnabled: {}", scansEnabled);
        queryEngine.setScansEnabled(scansEnabled);
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

    @Bean(name = "aerospikePersistenceEntityIndexCreator")
    public AerospikePersistenceEntityIndexCreator aerospikePersistenceEntityIndexCreator(
        ObjectProvider<AerospikeMappingContext> aerospikeMappingContext,
        AerospikeIndexResolver aerospikeIndexResolver,
        ObjectProvider<AerospikeTemplate> template, AerospikeSettings settings) {
        boolean indexesOnStartup = settings.getDataSettings().isCreateIndexesOnStartup();
        log.info("AerospikeDataSettings.indexesOnStartup: {}", indexesOnStartup);
        return new AerospikePersistenceEntityIndexCreator(aerospikeMappingContext, indexesOnStartup,
            aerospikeIndexResolver, template);
    }

    @Bean(name = "aerospikeIndexRefresher")
    public IndexRefresher indexRefresher(IAerospikeClient aerospikeClient, IndexesCacheUpdater indexesCacheUpdater,
                                         ServerVersionSupport serverVersionSupport, AerospikeSettings settings) {
        IndexRefresher refresher = new IndexRefresher(aerospikeClient, aerospikeClient.getInfoPolicyDefault(),
            new InternalIndexOperations(new IndexInfoParser()), indexesCacheUpdater, serverVersionSupport);
        refresher.refreshIndexes();
        int refreshFrequency = settings.getDataSettings().getIndexCacheRefreshSeconds();
        processCacheRefreshFrequency(refreshFrequency, refresher);
        log.info("AerospikeDataSettings.indexCacheRefreshSeconds: {}", refreshFrequency);
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
