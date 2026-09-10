package org.springframework.data.aerospike.examples.logical.blocking.programmatic.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.logical.blocking.programmatic.BlockingLogicalProgrammaticIndexedAndExample;
import org.springframework.data.aerospike.examples.logical.blocking.programmatic.repository.BlockingLogicalProgrammaticMovieRepository;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingLogicalProgrammaticMovieRepository.class)
public class BlockingLogicalProgrammaticIndexedConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingLogicalProgrammaticIndexedAndExample blockingLogicalProgrammaticIndexedAndExample(
        BlockingLogicalProgrammaticMovieRepository repository) {
        return new BlockingLogicalProgrammaticIndexedAndExample(repository);
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
