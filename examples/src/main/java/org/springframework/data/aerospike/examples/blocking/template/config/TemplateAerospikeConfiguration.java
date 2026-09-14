package org.springframework.data.aerospike.examples.blocking.template.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.template.BlockingTemplateExample;
import org.springframework.data.aerospike.examples.blocking.template.entity.TemplateMovieDocument;

@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:application.properties")
public class TemplateAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingTemplateExample blockingTemplateExample(AerospikeTemplate template) {
        return new BlockingTemplateExample(template);
    }

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
