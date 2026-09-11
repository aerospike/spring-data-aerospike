package org.springframework.data.aerospike.examples.combined.blocking.dsl.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.combined.blocking.dsl.BlockingDeclaredQueryConjunctionExample;
import org.springframework.data.aerospike.examples.combined.blocking.dsl.repository.BlockingDeclaredQueryRepository;
import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingDeclaredQueryRepository.class)
public class BlockingDeclaredQueryConjunctionConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingDeclaredQueryConjunctionExample blockingDeclaredQueryConjunctionExample(
        BlockingDeclaredQueryRepository repository) {
        return new BlockingDeclaredQueryConjunctionExample(repository);
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
