package org.springframework.data.aerospike.examples.combined.reactive.programmatic.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.reactive.programmatic.ReactiveCustomQueryDisjunctionExample;
import org.springframework.data.aerospike.examples.combined.reactive.programmatic.ReactiveCustomQueryNoIndexExample;
import org.springframework.data.aerospike.examples.combined.reactive.programmatic.repository.ReactiveCustomQueryRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveCustomQueryRepository.class)
public class ReactiveCustomQueryScanConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveCustomQueryDisjunctionExample reactiveCustomQueryDisjunctionExample(
        ReactiveCustomQueryRepository repository) {
        return new ReactiveCustomQueryDisjunctionExample(repository);
    }

    @Bean
    ReactiveCustomQueryNoIndexExample reactiveCustomQueryNoIndexExample(
        ReactiveCustomQueryRepository repository) {
        return new ReactiveCustomQueryNoIndexExample(repository);
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
