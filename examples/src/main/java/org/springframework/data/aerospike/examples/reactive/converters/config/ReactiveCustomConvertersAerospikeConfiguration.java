package org.springframework.data.aerospike.examples.reactive.converters.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.converters.ReactiveCustomConvertersExample;
import org.springframework.data.aerospike.examples.reactive.converters.entity.ReactiveConverterOrderDocument;
import org.springframework.data.aerospike.examples.reactive.converters.entity.ReactiveConverterOrderId;

import java.util.List;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ReactiveCustomConvertersExample.class)
public class ReactiveCustomConvertersAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return ReactiveConverterOrderDocument.class.getPackageName();
    }

    @Override
    protected List<Object> customConverters() {
        return List.of(
            ReactiveConverterOrderId.ReactiveConverterOrderIdToStringConverter.INSTANCE,
            ReactiveConverterOrderId.StringToReactiveConverterOrderIdConverter.INSTANCE
        );
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }
}
