package org.springframework.data.aerospike.examples.reactive.customquery.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.customquery.ReactiveProgrammaticCustomQueryExample;
import org.springframework.data.aerospike.examples.reactive.customquery.entity.ReactiveProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.reactive.customquery.repository.ReactiveProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ReactiveProgrammaticCustomQueryExample.class)
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveProgrammaticCustomQueryMovieRepository.class)
public class ReactiveProgrammaticCustomQueryConfiguration extends AbstractReactiveAerospikeDataConfiguration {

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
