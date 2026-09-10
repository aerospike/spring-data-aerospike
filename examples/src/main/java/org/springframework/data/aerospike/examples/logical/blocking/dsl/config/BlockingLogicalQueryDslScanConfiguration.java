package org.springframework.data.aerospike.examples.logical.blocking.dsl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.logical.blocking.dsl.BlockingLogicalQueryDslIndexedScanExample;
import org.springframework.data.aerospike.examples.logical.blocking.dsl.BlockingLogicalQueryDslNoIndexExample;
import org.springframework.data.aerospike.examples.logical.blocking.dsl.repository.BlockingLogicalQueryDslMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingLogicalQueryDslMovieRepository.class)
public class BlockingLogicalQueryDslScanConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingLogicalQueryDslIndexedScanExample blockingLogicalQueryDslIndexedScanExample(
        BlockingLogicalQueryDslMovieRepository repository) {
        return new BlockingLogicalQueryDslIndexedScanExample(repository);
    }

    @Bean
    BlockingLogicalQueryDslNoIndexExample blockingLogicalQueryDslNoIndexExample(
        BlockingLogicalQueryDslMovieRepository repository) {
        return new BlockingLogicalQueryDslNoIndexExample(repository);
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
