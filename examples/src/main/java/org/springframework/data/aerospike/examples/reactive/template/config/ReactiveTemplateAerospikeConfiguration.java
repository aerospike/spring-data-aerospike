package org.springframework.data.aerospike.examples.reactive.template.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.template.ReactiveTemplateExample;
import org.springframework.data.aerospike.examples.reactive.template.entity.ReactiveTemplateMovieDocument;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ReactiveTemplateExample.class)
public class ReactiveTemplateAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return ReactiveTemplateMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
