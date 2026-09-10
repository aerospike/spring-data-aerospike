package org.springframework.data.aerospike.examples.blocking.customquery.programmatic.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.ProgrammaticCustomQueryExample;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.entity.ProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.repository.ProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ProgrammaticCustomQueryExample.class)
@EnableAerospikeRepositories(basePackageClasses = ProgrammaticCustomQueryMovieRepository.class)
public class ProgrammaticCustomQueryConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return ProgrammaticCustomQueryMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
