package org.springframework.data.aerospike.examples.blocking.customquery.types.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.customquery.types.CustomQueryTypesExample;
import org.springframework.data.aerospike.examples.blocking.customquery.types.entity.CustomQueryTypesMovieDocument;
import org.springframework.data.aerospike.examples.blocking.customquery.types.repository.CustomQueryTypesMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;

// tag::custom-query-types-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableAerospikeRepositories(basePackageClasses = CustomQueryTypesMovieRepository.class)
// Enables the repository proxy and template support used by qualifier type examples.
public class CustomQueryTypesConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    CustomQueryTypesExample customQueryTypesExample(CustomQueryTypesMovieRepository repository,
                                                    AerospikeTemplate template,
                                                    ServerVersionSupport serverVersionSupport) {
        return new CustomQueryTypesExample(repository, template, serverVersionSupport);
    }

    @Override
    protected String getMappingBasePackage() {
        return CustomQueryTypesMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
// end::custom-query-types-configuration[]
