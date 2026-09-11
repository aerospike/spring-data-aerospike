package org.springframework.data.aerospike.examples.combined.blocking.derived.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.combined.blocking.derived.BlockingDerivedQueryConjunctionExample;
import org.springframework.data.aerospike.examples.combined.blocking.derived.repository.BlockingDerivedQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingDerivedQueryRepository.class)
public class BlockingDerivedQueryConjunctionConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingDerivedQueryConjunctionExample blockingDerivedQueryConjunctionExample(
        BlockingDerivedQueryRepository repository) {
        return new BlockingDerivedQueryConjunctionExample(repository);
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
