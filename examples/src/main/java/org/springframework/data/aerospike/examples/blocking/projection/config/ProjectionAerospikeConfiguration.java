package org.springframework.data.aerospike.examples.blocking.projection.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.projection.ProjectionExample;
import org.springframework.data.aerospike.examples.blocking.projection.entity.ProjectedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.projection.repository.ProjectionMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

// tag::projection-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableAerospikeRepositories(basePackageClasses = ProjectionMovieRepository.class)
// Enables the repository proxy that returns projection interfaces and DTOs.
public class ProjectionAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    ProjectionExample projectionExample(ProjectionMovieRepository repository) {
        return new ProjectionExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return ProjectedMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
    }
}
// end::projection-configuration[]
