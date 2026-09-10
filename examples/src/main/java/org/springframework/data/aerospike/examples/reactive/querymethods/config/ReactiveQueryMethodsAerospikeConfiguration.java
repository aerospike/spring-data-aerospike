package org.springframework.data.aerospike.examples.reactive.querymethods.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.querymethods.ReactiveRepositoryQueryMethodsExample;
import org.springframework.data.aerospike.examples.reactive.querymethods.entity.ReactiveQueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.reactive.querymethods.repository.ReactiveQueryMethodsMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ReactiveRepositoryQueryMethodsExample.class)
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveQueryMethodsMovieRepository.class)
public class ReactiveQueryMethodsAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

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
