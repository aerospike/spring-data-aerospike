package org.springframework.data.aerospike.examples.combined.blocking.dsl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.combined.blocking.dsl.BlockingDeclaredQueryDisjunctionExample;
import org.springframework.data.aerospike.examples.combined.blocking.dsl.BlockingDeclaredQueryNoIndexExample;
import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingDeclaredQueryRepository.class)
public class BlockingDeclaredQueryScanConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingDeclaredQueryDisjunctionExample blockingDeclaredQueryDisjunctionExample(
        BlockingDeclaredQueryRepository repository) {
        return new BlockingDeclaredQueryDisjunctionExample(repository);
    }

    @Bean
    BlockingDeclaredQueryNoIndexExample blockingDeclaredQueryNoIndexExample(
        BlockingDeclaredQueryRepository repository) {
        return new BlockingDeclaredQueryNoIndexExample(repository);
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
