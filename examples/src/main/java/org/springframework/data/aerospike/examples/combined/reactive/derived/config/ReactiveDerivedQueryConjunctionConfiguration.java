package org.springframework.data.aerospike.examples.combined.reactive.derived.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.examples.combined.reactive.derived.ReactiveDerivedQueryConjunctionExample;
import org.springframework.data.aerospike.examples.combined.reactive.derived.repository.ReactiveDerivedQueryRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveDerivedQueryRepository.class)
public class ReactiveDerivedQueryConjunctionConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveDerivedQueryConjunctionExample reactiveDerivedQueryConjunctionExample(
        ReactiveDerivedQueryRepository repository) {
        return new ReactiveDerivedQueryConjunctionExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return Movie.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
