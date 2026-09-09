package org.springframework.data.aerospike.examples.reactive.crud.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.crud.ReactiveRepositoryCrudExample;
import org.springframework.data.aerospike.examples.reactive.crud.entity.ReactiveMovieDocument;
import org.springframework.data.aerospike.examples.reactive.crud.repository.ReactiveMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ReactiveRepositoryCrudExample.class)
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveMovieRepository.class)
public class ReactiveAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return ReactiveMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
    }
}
