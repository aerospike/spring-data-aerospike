package org.springframework.data.aerospike.examples.blocking.query.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.query.SecondaryIndexQueryExample;
import org.springframework.data.aerospike.examples.blocking.query.entity.IndexedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.query.repository.IndexedMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

// tag::secondary-index-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableAerospikeRepositories(basePackageClasses = IndexedMovieRepository.class)
// Enables the repository proxy that runs the secondary-index query example.
public class QueryAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    SecondaryIndexQueryExample secondaryIndexQueryExample(IndexedMovieRepository repository,
                                                          AerospikeTemplate template) {
        return new SecondaryIndexQueryExample(repository, template);
    }

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
// end::secondary-index-configuration[]
