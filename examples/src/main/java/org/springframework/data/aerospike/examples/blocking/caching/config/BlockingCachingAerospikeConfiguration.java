package org.springframework.data.aerospike.examples.blocking.caching.config;

import com.aerospike.client.IAerospikeClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.cache.AerospikeCacheConfiguration;
import org.springframework.data.aerospike.cache.AerospikeCacheKeyProcessor;
import org.springframework.data.aerospike.cache.AerospikeCacheManager;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.examples.blocking.caching.BlockingCachingExample;
import org.springframework.data.aerospike.examples.blocking.caching.BlockingCachingMovieService;
import org.springframework.data.aerospike.examples.blocking.caching.entity.CacheEntryDocument;
import org.springframework.data.aerospike.examples.blocking.caching.entity.CachedMovie;

// tag::caching-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableCaching
// Enables Spring cache advice and wires an Aerospike-backed CacheManager.
public class BlockingCachingAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    // tag::caching-cache-manager[]
    // AerospikeCacheManager stores cache entries in a dedicated example cache set.
    @Bean
    CacheManager cacheManager(IAerospikeClient client, MappingAerospikeConverter converter,
                              AerospikeCacheKeyProcessor cacheKeyProcessor,
                              @Value("${spring.data.aerospike.namespace:test}") String namespace) {
        AerospikeCacheConfiguration cacheConfiguration =
            new AerospikeCacheConfiguration(namespace, CacheEntryDocument.SET_NAME);
        return new AerospikeCacheManager(client, converter, cacheConfiguration, cacheKeyProcessor);
    }
    // end::caching-cache-manager[]

    // The service is a Spring bean so cache annotations are applied through proxy advice.
    @Bean
    BlockingCachingMovieService blockingCachingMovieService() {
        return new BlockingCachingMovieService();
    }

    @Bean
    BlockingCachingExample blockingCachingExample(BlockingCachingMovieService service, CacheManager cacheManager) {
        return new BlockingCachingExample(service, cacheManager);
    }

    @Override
    protected String getMappingBasePackage() {
        return CachedMovie.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
// end::caching-configuration[]
