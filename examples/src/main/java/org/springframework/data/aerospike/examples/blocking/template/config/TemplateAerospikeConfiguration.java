package org.springframework.data.aerospike.examples.blocking.template.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.template.BlockingTemplateExample;
import org.springframework.data.aerospike.examples.blocking.template.entity.TemplateMovieDocument;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = BlockingTemplateExample.class)
public class TemplateAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

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
