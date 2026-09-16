package org.springframework.data.aerospike.examples.blocking.indexed.context.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.indexed.context.IndexedContextExample;
import org.springframework.data.aerospike.examples.blocking.indexed.context.entity.IndexedPersonDocument;
import org.springframework.data.aerospike.examples.blocking.indexed.context.repository.IndexedContextPersonRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

// tag::indexed-context-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableAerospikeRepositories(basePackageClasses = IndexedContextPersonRepository.class)
// Enables the repository proxy while startup index creation reads nested context metadata.
public class IndexedContextConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    IndexedContextExample indexedContextExample(IndexedContextPersonRepository repository) {
        return new IndexedContextExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return IndexedPersonDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(true);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
// end::indexed-context-configuration[]
