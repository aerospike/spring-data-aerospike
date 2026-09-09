package org.springframework.data.aerospike.examples.blocking.projection.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.projection.ProjectionExample;
import org.springframework.data.aerospike.examples.blocking.projection.entity.ProjectedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.projection.repository.ProjectionMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ProjectionExample.class)
@EnableAerospikeRepositories(basePackageClasses = ProjectionMovieRepository.class)
public class ProjectionAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return ProjectedMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
    }
}
