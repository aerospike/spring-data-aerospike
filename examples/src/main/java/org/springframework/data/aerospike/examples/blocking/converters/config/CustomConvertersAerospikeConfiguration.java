package org.springframework.data.aerospike.examples.blocking.converters.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.converters.BlockingCustomConvertersExample;
import org.springframework.data.aerospike.examples.blocking.converters.entity.ConverterOrderDocument;
import org.springframework.data.aerospike.examples.blocking.converters.entity.ConverterOrderId;

import java.util.List;

// tag::custom-converters-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
// Registers custom id converters and maps only the converter example documents.
public class CustomConvertersAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingCustomConvertersExample blockingCustomConvertersExample(AerospikeTemplate template) {
        return new BlockingCustomConvertersExample(template);
    }

    // Restrict mapping so the converter example scans only its document package.
    @Override
    protected String getMappingBasePackage() {
        return ConverterOrderDocument.class.getPackageName();
    }

    // tag::custom-converters-registration[]
    @Override
    protected List<Object> customConverters() {
        return List.of(
            ConverterOrderId.ConverterOrderIdToStringConverter.INSTANCE,
            ConverterOrderId.StringToConverterOrderIdConverter.INSTANCE
        );
    }
    // end::custom-converters-registration[]

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
// end::custom-converters-configuration[]
