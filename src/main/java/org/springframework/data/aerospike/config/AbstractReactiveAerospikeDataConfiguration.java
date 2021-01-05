package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
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
    public AerospikeReactorClient aerospikeReactorClient(AerospikeClient aerospikeClient, EventLoops eventLoops) {
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
