package org.springframework.data.aerospike.examples.blocking.customquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.customquery.CustomQueryDslExample;
import org.springframework.data.aerospike.examples.blocking.customquery.entity.CustomQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.repository.CustomQueryMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = CustomQueryMovieRepository.class)
public class CustomQueryConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    CustomQueryDslExample customQueryDslExample(CustomQueryMovieRepository repository) {
        return new CustomQueryDslExample(repository);
    }

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
