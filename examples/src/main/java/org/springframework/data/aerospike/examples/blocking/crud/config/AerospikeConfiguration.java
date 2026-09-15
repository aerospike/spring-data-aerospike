package org.springframework.data.aerospike.examples.blocking.crud.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.crud.BlockingRepositoryCrudExample;
import org.springframework.data.aerospike.examples.blocking.crud.entity.MovieDocument;
import org.springframework.data.aerospike.examples.blocking.crud.repository.MovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

// tag::blocking-crud-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = MovieRepository.class)
// Enables the blocking repository proxy used by the CRUD example bean.
public class AerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    // tag::blocking-crud-example-bean[]
    @Bean
    BlockingRepositoryCrudExample blockingRepositoryCrudExample(MovieRepository repository) {
        return new BlockingRepositoryCrudExample(repository);
    }
    // end::blocking-crud-example-bean[]

    @Override
    protected String getMappingBasePackage() {
        return MovieDocument.class.getPackageName();
    }

    // tag::blocking-crud-data-settings[]
    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
    // end::blocking-crud-data-settings[]
}
// end::blocking-crud-configuration[]
