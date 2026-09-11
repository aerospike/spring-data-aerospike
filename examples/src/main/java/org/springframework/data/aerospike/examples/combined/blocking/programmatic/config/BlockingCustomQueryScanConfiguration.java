package org.springframework.data.aerospike.examples.combined.blocking.programmatic.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.combined.blocking.programmatic.BlockingCustomQueryDisjunctionExample;
import org.springframework.data.aerospike.examples.combined.blocking.programmatic.BlockingCustomQueryNoIndexExample;
import org.springframework.data.aerospike.examples.combined.blocking.programmatic.repository.BlockingCustomQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingCustomQueryRepository.class)
public class BlockingCustomQueryScanConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingCustomQueryDisjunctionExample blockingCustomQueryDisjunctionExample(
        BlockingCustomQueryRepository repository) {
        return new BlockingCustomQueryDisjunctionExample(repository);
    }

    @Bean
    BlockingCustomQueryNoIndexExample blockingCustomQueryNoIndexExample(
        BlockingCustomQueryRepository repository) {
        return new BlockingCustomQueryNoIndexExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return Movie.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
