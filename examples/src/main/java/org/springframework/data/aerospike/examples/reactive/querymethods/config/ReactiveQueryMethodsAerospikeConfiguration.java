package org.springframework.data.aerospike.examples.reactive.querymethods.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.querymethods.ReactiveRepositoryDerivedIdAndBinQueryExample;
import org.springframework.data.aerospike.examples.reactive.querymethods.ReactiveRepositoryQueryMethodsExample;
import org.springframework.data.aerospike.examples.reactive.querymethods.entity.ReactiveQueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.reactive.querymethods.repository.ReactiveQueryMethodsMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveQueryMethodsMovieRepository.class)
// Enables the reactive repository proxy that derives query methods from method names.
public class ReactiveQueryMethodsAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveRepositoryQueryMethodsExample reactiveRepositoryQueryMethodsExample(
        ReactiveQueryMethodsMovieRepository repository) {
        return new ReactiveRepositoryQueryMethodsExample(repository);
    }

    @Bean
    ReactiveRepositoryDerivedIdAndBinQueryExample reactiveRepositoryDerivedIdAndBinQueryExample(
        ReactiveQueryMethodsMovieRepository repository) {
        return new ReactiveRepositoryDerivedIdAndBinQueryExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return ReactiveQueryMethodsMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
