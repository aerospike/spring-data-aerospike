package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.reactor.AerospikeReactorClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;

/**
 * Configuration with beans needed for reactive stuff
 *
 * @author Igor Ermolenko
 */
@Configuration
public abstract class AbstractReactiveAerospikeDataConfiguration extends AbstractAerospikeDataConfiguration {
    @Bean(name = "reactiveAerospikeTemplate")
    public ReactiveAerospikeTemplate reactiveAerospikeTemplate(AerospikeClient aerospikeClient,
                                                               MappingAerospikeConverter mappingAerospikeConverter,
                                                               AerospikeMappingContext aerospikeMappingContext,
                                                               AerospikeExceptionTranslator aerospikeExceptionTranslator,
                                                               AerospikeReactorClient aerospikeReactorClient) {
        return new ReactiveAerospikeTemplate(aerospikeClient, nameSpace(), mappingAerospikeConverter, aerospikeMappingContext, aerospikeExceptionTranslator, aerospikeReactorClient);
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
}
