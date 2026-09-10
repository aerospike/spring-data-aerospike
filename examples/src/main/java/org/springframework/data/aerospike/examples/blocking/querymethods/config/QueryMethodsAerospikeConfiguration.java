package org.springframework.data.aerospike.examples.blocking.querymethods.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.querymethods.BlockingRepositoryQueryMethodsExample;
import org.springframework.data.aerospike.examples.blocking.querymethods.entity.QueryMethodsMovieDocument;
import org.springframework.data.aerospike.examples.blocking.querymethods.repository.QueryMethodsMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = BlockingRepositoryQueryMethodsExample.class)
@EnableAerospikeRepositories(basePackageClasses = QueryMethodsMovieRepository.class)
public class QueryMethodsAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return QueryMethodsMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
