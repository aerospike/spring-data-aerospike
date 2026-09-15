package org.springframework.data.aerospike.examples.reactive.crud.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.crud.ReactiveRepositoryCrudExample;
import org.springframework.data.aerospike.examples.reactive.crud.entity.ReactiveMovieDocument;
import org.springframework.data.aerospike.examples.reactive.crud.repository.ReactiveMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

// tag::reactive-crud-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveMovieRepository.class)
// Enables the reactive repository proxy used by the CRUD example bean.
public class ReactiveAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    // tag::reactive-crud-example-bean[]
    @Bean
    ReactiveRepositoryCrudExample reactiveRepositoryCrudExample(ReactiveMovieRepository repository) {
        return new ReactiveRepositoryCrudExample(repository);
    }
    // end::reactive-crud-example-bean[]

    @Override
    protected String getMappingBasePackage() {
        return ReactiveMovieDocument.class.getPackageName();
    }

    // tag::reactive-crud-data-settings[]
    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
    // end::reactive-crud-data-settings[]
}
// end::reactive-crud-configuration[]
