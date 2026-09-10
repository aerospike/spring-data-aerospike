package org.springframework.data.aerospike.examples.blocking.crud.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.crud.BlockingRepositoryCrudExample;
import org.springframework.data.aerospike.examples.blocking.crud.entity.MovieDocument;
import org.springframework.data.aerospike.examples.blocking.crud.repository.MovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = BlockingRepositoryCrudExample.class)
@EnableAerospikeRepositories(basePackageClasses = MovieRepository.class)
public class AerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return MovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
