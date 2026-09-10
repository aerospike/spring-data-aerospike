package org.springframework.data.aerospike.examples.logical.reactive.derived.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.examples.logical.reactive.derived.ReactiveLogicalDerivedIndexedANDExample;
import org.springframework.data.aerospike.examples.logical.reactive.derived.repository.ReactiveLogicalDerivedMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveLogicalDerivedMovieRepository.class)
public class ReactiveLogicalDerivedIndexedConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveLogicalDerivedIndexedANDExample reactiveLogicalDerivedIndexedAndExample(
        ReactiveLogicalDerivedMovieRepository repository) {
        return new ReactiveLogicalDerivedIndexedANDExample(repository);
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
