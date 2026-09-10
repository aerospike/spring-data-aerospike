package org.springframework.data.aerospike.examples.blocking.converters.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.converters.BlockingCustomConvertersExample;
import org.springframework.data.aerospike.examples.blocking.converters.entity.ConverterOrderDocument;
import org.springframework.data.aerospike.examples.blocking.converters.entity.ConverterOrderId;

import java.util.List;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = BlockingCustomConvertersExample.class)
public class CustomConvertersAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return ConverterOrderDocument.class.getPackageName();
    }

    @Override
    protected List<Object> customConverters() {
        return List.of(
            ConverterOrderId.ConverterOrderIdToStringConverter.INSTANCE,
            ConverterOrderId.StringToConverterOrderIdConverter.INSTANCE
        );
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
