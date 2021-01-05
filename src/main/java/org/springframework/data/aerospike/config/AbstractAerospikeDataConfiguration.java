package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.index.AerospikePersistenceEntityIndexCreator;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.query.cache.IndexesCacheUpdater;
import org.springframework.data.aerospike.query.cache.InternalIndexOperations;

@Configuration
public abstract class AbstractAerospikeDataConfiguration extends AerospikeDataConfigurationSupport {

    @Bean(name = "aerospikeTemplate")
    public AerospikeTemplate aerospikeTemplate(AerospikeClient aerospikeClient,
                                               MappingAerospikeConverter mappingAerospikeConverter,
                                               AerospikeMappingContext aerospikeMappingContext,
                                               AerospikeExceptionTranslator aerospikeExceptionTranslator,
                                               QueryEngine queryEngine, IndexRefresher indexRefresher) {
        return new AerospikeTemplate(aerospikeClient, nameSpace(), mappingAerospikeConverter,
                aerospikeMappingContext, aerospikeExceptionTranslator, queryEngine, indexRefresher);
    }

    @Bean(name = "aerospikeQueryEngine")
    public QueryEngine queryEngine(AerospikeClient aerospikeClient,
                                   StatementBuilder statementBuilder) {
        QueryEngine queryEngine = new QueryEngine(aerospikeClient, statementBuilder, aerospikeClient.getQueryPolicyDefault());
        queryEngine.setScansEnabled(aerospikeDataSettings().isScansEnabled());
        return queryEngine;
    }

    @Bean
    public AerospikePersistenceEntityIndexCreator aerospikePersistenceEntityIndexCreator(AerospikeMappingContext aerospikeMappingContext,
                                                                                         @Lazy AerospikeTemplate template) {
        return new AerospikePersistenceEntityIndexCreator(aerospikeMappingContext, template);
    }

    @Bean(name = "aerospikeIndexRefresher")
    public IndexRefresher indexRefresher(AerospikeClient aerospikeClient, IndexesCacheUpdater indexesCacheUpdater) {
        IndexRefresher refresher = new IndexRefresher(aerospikeClient, aerospikeClient.getInfoPolicyDefault(), new InternalIndexOperations(new IndexInfoParser()), indexesCacheUpdater);
        refresher.refreshIndexes();
        return refresher;
    }
}
