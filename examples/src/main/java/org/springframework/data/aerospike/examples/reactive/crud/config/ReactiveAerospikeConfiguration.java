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

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveMovieRepository.class)
public class ReactiveAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveRepositoryCrudExample reactiveRepositoryCrudExample(ReactiveMovieRepository repository) {
        return new ReactiveRepositoryCrudExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return ReactiveMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
