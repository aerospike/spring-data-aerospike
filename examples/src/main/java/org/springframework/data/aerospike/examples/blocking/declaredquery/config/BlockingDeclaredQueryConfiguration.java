package org.springframework.data.aerospike.examples.blocking.declaredquery.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.declaredquery.BlockingDeclaredQueryExample;
import org.springframework.data.aerospike.examples.blocking.declaredquery.entity.DeclaredQueryMovieDocument;
import org.springframework.data.aerospike.examples.blocking.declaredquery.repository.DeclaredQueryMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = DeclaredQueryMovieRepository.class)
// Enables the repository proxy that parses declared @Query DSL methods.
public class BlockingDeclaredQueryConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingDeclaredQueryExample blockingDeclaredQueryExample(DeclaredQueryMovieRepository repository) {
        return new BlockingDeclaredQueryExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return DeclaredQueryMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
