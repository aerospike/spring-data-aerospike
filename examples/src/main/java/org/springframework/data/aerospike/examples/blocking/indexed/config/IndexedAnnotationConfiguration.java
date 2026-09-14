package org.springframework.data.aerospike.examples.blocking.indexed.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.indexed.IndexedAnnotationExample;
import org.springframework.data.aerospike.examples.blocking.indexed.entity.AnnotatedMovieDocument;
import org.springframework.data.aerospike.examples.blocking.indexed.repository.AnnotatedMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
@EnableAerospikeRepositories(basePackageClasses = AnnotatedMovieRepository.class)
public class IndexedAnnotationConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    IndexedAnnotationExample indexedAnnotationExample(AnnotatedMovieRepository repository) {
        return new IndexedAnnotationExample(repository);
    }

    @Override
    protected String getMappingBasePackage() {
        return AnnotatedMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(true);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
