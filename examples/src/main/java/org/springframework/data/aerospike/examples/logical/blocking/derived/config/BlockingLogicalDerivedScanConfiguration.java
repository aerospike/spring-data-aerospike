package org.springframework.data.aerospike.examples.logical.blocking.derived.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.logical.blocking.derived.BlockingLogicalDerivedIndexedScanExample;
import org.springframework.data.aerospike.examples.logical.blocking.derived.BlockingLogicalDerivedNoIndexExample;
import org.springframework.data.aerospike.examples.logical.blocking.derived.repository.BlockingLogicalDerivedMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingLogicalDerivedMovieRepository.class)
public class BlockingLogicalDerivedScanConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingLogicalDerivedIndexedScanExample blockingLogicalDerivedIndexedScanExample(
        BlockingLogicalDerivedMovieRepository repository) {
        return new BlockingLogicalDerivedIndexedScanExample(repository);
    }

    @Bean
    BlockingLogicalDerivedNoIndexExample blockingLogicalDerivedNoIndexExample(
        BlockingLogicalDerivedMovieRepository repository) {
        return new BlockingLogicalDerivedNoIndexExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return LogicalMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
