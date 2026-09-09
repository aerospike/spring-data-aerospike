package org.springframework.data.aerospike.examples.blocking.customquery.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.customquery.CustomQueryDslExample;
import org.springframework.data.aerospike.examples.blocking.customquery.entity.CustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.repository.CustomQueryMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = CustomQueryDslExample.class)
@EnableAerospikeRepositories(basePackageClasses = CustomQueryMovieRepository.class)
public class CustomQueryConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return CustomQueryMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
