package org.springframework.data.aerospike.examples.blocking.template.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.template.BlockingTemplateExample;
import org.springframework.data.aerospike.examples.blocking.template.entity.TemplateMovieDocument;

// tag::template-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
// Keeps the blocking template example focused on the mapped movie document package.
public class TemplateAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    // tag::template-bean[]
    @Bean
    BlockingTemplateExample blockingTemplateExample(AerospikeTemplate template) {
        return new BlockingTemplateExample(template);
    }
    // end::template-bean[]

    @Override
    protected String getMappingBasePackage() {
        return TemplateMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(false);
    }
}
// end::template-configuration[]
