package org.springframework.data.aerospike.examples.logical.reactive.programmatic.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.examples.logical.reactive.programmatic.ReactiveLogicalProgrammaticIndexedANDExample;
import org.springframework.data.aerospike.examples.logical.reactive.programmatic.repository.ReactiveLogicalProgrammaticMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveLogicalProgrammaticMovieRepository.class)
public class ReactiveLogicalProgrammaticIndexedConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveLogicalProgrammaticIndexedANDExample reactiveLogicalProgrammaticIndexedAndExample(
        ReactiveLogicalProgrammaticMovieRepository repository) {
        return new ReactiveLogicalProgrammaticIndexedANDExample(repository);
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
