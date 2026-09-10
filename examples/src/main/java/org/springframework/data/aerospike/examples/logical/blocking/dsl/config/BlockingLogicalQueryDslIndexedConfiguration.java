package org.springframework.data.aerospike.examples.logical.blocking.dsl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.logical.blocking.dsl.BlockingLogicalQueryDslIndexedANDExample;
import org.springframework.data.aerospike.examples.logical.blocking.dsl.repository.BlockingLogicalQueryDslMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingLogicalQueryDslMovieRepository.class)
public class BlockingLogicalQueryDslIndexedConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingLogicalQueryDslIndexedANDExample blockingLogicalQueryDslIndexedAndExample(
        BlockingLogicalQueryDslMovieRepository repository) {
        return new BlockingLogicalQueryDslIndexedANDExample(repository);
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
