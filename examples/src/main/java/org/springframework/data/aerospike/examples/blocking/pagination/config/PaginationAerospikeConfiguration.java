package org.springframework.data.aerospike.examples.blocking.pagination.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.pagination.PaginationAndSortingExample;
import org.springframework.data.aerospike.examples.blocking.pagination.entity.PaginationMovieDocument;
import org.springframework.data.aerospike.examples.blocking.pagination.repository.PaginationMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

// tag::pagination-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableAerospikeRepositories(basePackageClasses = PaginationMovieRepository.class)
// Enables the repository proxy that accepts Sort, Page, and Slice arguments.
public class PaginationAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    PaginationAndSortingExample paginationAndSortingExample(PaginationMovieRepository repository) {
        return new PaginationAndSortingExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return PaginationMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
// end::pagination-configuration[]
