package org.springframework.data.aerospike.examples.combined.reactive.programmatic.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.reactive.programmatic.ReactiveCustomQueryConjunctionExample;
import org.springframework.data.aerospike.examples.combined.reactive.programmatic.repository.ReactiveCustomQueryRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveCustomQueryRepository.class)
public class ReactiveCustomQueryConjunctionConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveCustomQueryConjunctionExample reactiveCustomQueryConjunctionExample(
        ReactiveCustomQueryRepository repository) {
        return new ReactiveCustomQueryConjunctionExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return Movie.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
