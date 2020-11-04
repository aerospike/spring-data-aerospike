package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.cache.AerospikeCacheManager;
import org.springframework.data.aerospike.cache.AerospikeCacheManagerIntegrationTests.CachingComponent;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.query.QueryEngineTestDataPopulator;

/**
 * @author Taras Danylchuk
 */
@Configuration
@EnableCaching
@EnableAutoConfiguration
public class CommonTestConfig {

    @Value("${embedded.aerospike.namespace}")
    protected String namespace;

    @Bean
    public CacheManager cacheManager(AerospikeClient aerospikeClient, MappingAerospikeConverter aerospikeConverter) {
        return new AerospikeCacheManager(aerospikeClient, aerospikeConverter);
    }

    @Bean
    public CachingComponent cachingComponent() {
        return new CachingComponent();
    }

    @Bean
    public QueryEngineTestDataPopulator queryEngineTestDataPopulator(AerospikeClient client) {
        return new QueryEngineTestDataPopulator(namespace, client);
    }

}
