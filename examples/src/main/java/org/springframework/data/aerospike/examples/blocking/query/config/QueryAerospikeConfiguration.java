package org.springframework.data.aerospike.examples.blocking.query.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.query.SecondaryIndexQueryExample;
import org.springframework.data.aerospike.examples.blocking.query.entity.IndexedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.query.repository.IndexedMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = SecondaryIndexQueryExample.class)
@EnableAerospikeRepositories(basePackageClasses = IndexedMovieRepository.class)
public class QueryAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return IndexedMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
