package org.springframework.data.aerospike.examples.reactive.template.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.examples.reactive.template.ReactiveTemplateExample;
import org.springframework.data.aerospike.examples.reactive.template.entity.ReactiveTemplateMovieDocument;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
public class ReactiveTemplateAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveTemplateExample reactiveTemplateExample(ReactiveAerospikeTemplate template) {
        return new ReactiveTemplateExample(template);
    }

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
