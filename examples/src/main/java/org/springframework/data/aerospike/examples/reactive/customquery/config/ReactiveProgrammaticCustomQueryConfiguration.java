package org.springframework.data.aerospike.examples.reactive.customquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.customquery.ReactiveProgrammaticCustomQueryExample;
import org.springframework.data.aerospike.examples.reactive.customquery.ReactiveProgrammaticCustomQueryIdAndBinExample;
import org.springframework.data.aerospike.examples.reactive.customquery.entity.ReactiveProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.reactive.customquery.repository.ReactiveProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveProgrammaticCustomQueryMovieRepository.class)
// Enables the reactive repository proxy that accepts programmatic Query objects.
public class ReactiveProgrammaticCustomQueryConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveProgrammaticCustomQueryExample reactiveProgrammaticCustomQueryExample(
        ReactiveProgrammaticCustomQueryMovieRepository repository) {
        return new ReactiveProgrammaticCustomQueryExample(repository);
    }

    @Bean
    ReactiveProgrammaticCustomQueryIdAndBinExample reactiveProgrammaticCustomQueryIdAndBinExample(
        ReactiveProgrammaticCustomQueryMovieRepository repository) {
        return new ReactiveProgrammaticCustomQueryIdAndBinExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return ReactiveProgrammaticCustomQueryMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
