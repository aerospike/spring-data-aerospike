package org.springframework.data.aerospike.examples.blocking.customquery.programmatic.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.ProgrammaticCustomQueryExample;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.ProgrammaticCustomQueryIdAndBinExample;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.entity.ProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.repository.ProgrammaticCustomQueryMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableAerospikeRepositories(basePackageClasses = ProgrammaticCustomQueryMovieRepository.class)
// Enables the repository proxy that accepts programmatic Query objects.
public class ProgrammaticCustomQueryConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    ProgrammaticCustomQueryExample programmaticCustomQueryExample(
        ProgrammaticCustomQueryMovieRepository repository) {
        return new ProgrammaticCustomQueryExample(repository);
    }

    @Bean
    ProgrammaticCustomQueryIdAndBinExample programmaticCustomQueryIdAndBinExample(
        ProgrammaticCustomQueryMovieRepository repository) {
        return new ProgrammaticCustomQueryIdAndBinExample(repository);
    }

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
