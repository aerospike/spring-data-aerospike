package org.springframework.data.aerospike.examples.logical.blocking.derived.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.logical.blocking.derived.BlockingLogicalDerivedIndexedANDExample;
import org.springframework.data.aerospike.examples.logical.blocking.derived.repository.BlockingLogicalDerivedMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingLogicalDerivedMovieRepository.class)
public class BlockingLogicalDerivedIndexedConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingLogicalDerivedIndexedANDExample blockingLogicalDerivedIndexedAndExample(
        BlockingLogicalDerivedMovieRepository repository) {
        return new BlockingLogicalDerivedIndexedANDExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return LogicalMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
