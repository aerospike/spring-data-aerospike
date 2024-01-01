package org.springframework.data.aerospike.convert;

import com.aerospike.client.Bin;
import com.aerospike.client.Record;
import org.springframework.context.ApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.config.AerospikeSettings;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.convert.CustomConversions;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class BaseMappingAerospikeConverterTest {

    protected static final String NAMESPACE = "namespace";
    public final AerospikeSettings settings = new AerospikeSettings();
    public final AerospikeSettings settingsReversedKeyTypesOption =
        getAerospikeSettings(!settings.isKeepOriginalKeyTypes());

    private AerospikeSettings getAerospikeSettings(boolean keepOriginalKeyTypes) {
        AerospikeSettings settings = new AerospikeSettings();
        settings.setKeepOriginalKeyTypes(keepOriginalKeyTypes);
        return settings;
    }

    protected final MappingAerospikeConverter converter = getMappingAerospikeConverter(
        settings,
        new SampleClasses.ComplexIdToStringConverter(),
        new SampleClasses.StringToComplexIdConverter());

    protected final MappingAerospikeConverter converterReversedKeyTypes = getMappingAerospikeConverter(
        settingsReversedKeyTypesOption,
        new SampleClasses.ComplexIdToStringConverter(),
        new SampleClasses.StringToComplexIdConverter());

    protected static Record aeroRecord(Collection<Bin> bins) {
        Map<String, Object> collect = bins.stream()
            .collect(Collectors.toMap(bin -> bin.name, bin -> bin.value.getObject()));
        return aeroRecord(collect);
    }

    protected static Record aeroRecord(Map<String, Object> bins) {
        return new Record(bins, 0, 0);
    }

    protected MappingAerospikeConverter getAerospikeMappingConverterByOption(int converterOption) {
        if (converterOption == 0) {
            return converter;
        }
        return converterReversedKeyTypes;
    }

    protected MappingAerospikeConverter getMappingAerospikeConverter(AerospikeSettings settings,
                                                                     Converter<?, ?>... customConverters) {
        return getMappingAerospikeConverter(settings, new AerospikeTypeAliasAccessor(), customConverters);
    }

    protected MappingAerospikeConverter getMappingAerospikeConverter(AerospikeSettings settings,
                                                                     AerospikeTypeAliasAccessor typeAliasAccessor,
                                                                     Converter<?, ?>... customConverters) {
        AerospikeMappingContext mappingContext = new AerospikeMappingContext();
        mappingContext.setApplicationContext(getApplicationContext());
        CustomConversions customConversions = new AerospikeCustomConversions(asList(customConverters));

        MappingAerospikeConverter converter = new MappingAerospikeConverter(mappingContext, customConversions,
            typeAliasAccessor, settings);
        converter.afterPropertiesSet();
        return converter;
    }

    private ApplicationContext getApplicationContext() {
        Environment environment = mock(Environment.class);
        when(environment.resolveRequiredPlaceholders(anyString()))
            .thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

        ApplicationContext applicationContext = mock(ApplicationContext.class);
        when(applicationContext.getEnvironment()).thenReturn(environment);

        return applicationContext;
    }

    protected static Object getBinValue(String name, Collection<Bin> bins) {
        if (bins == null || bins.isEmpty())
            return null;

        return bins.stream()
            .filter(bin -> bin.name.equals(name))
            .map(bin -> bin.value.getObject())
            .findFirst().orElse(null);
    }
}
