package org.springframework.data.aerospike;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.aerospike.config.AerospikeDataConfigurationSupport;
import org.springframework.data.aerospike.convert.AerospikeCustomConversions;
import org.springframework.data.aerospike.convert.AerospikeCustomConverters;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AerospikeDataConfigurationSupportTests {

    @WritingConverter
    enum IntToStringConverter implements Converter<Integer, String> {
        INSTANCE;

        @Override
        public String convert(Integer source) {
            return source.toString();
        }
    }

    @ReadingConverter
    enum StringToIntConverter implements Converter<String, Integer> {
        INSTANCE;

        @Override
        public Integer convert(String source) {
            return Integer.valueOf(source);
        }
    }

    @WritingConverter
    enum LongToStringConverter implements Converter<Long, String> {
        INSTANCE;

        @Override
        public String convert(Long source) {
            return source.toString();
        }
    }

    @ReadingConverter
    enum StringToLongConverter implements Converter<String, Long> {
        INSTANCE;

        @Override
        public Long convert(String source) {
            return Long.valueOf(source);
        }
    }

    @SuppressWarnings("unchecked")
    private ObjectProvider<AerospikeCustomConverters> emptyProvider() {
        ObjectProvider<AerospikeCustomConverters> provider = mock(ObjectProvider.class);
        when(provider.orderedStream()).thenReturn(Stream.empty());
        return provider;
    }

    @SuppressWarnings("unchecked")
    private ObjectProvider<AerospikeCustomConverters> providerOf(AerospikeCustomConverters... beans) {
        ObjectProvider<AerospikeCustomConverters> provider = mock(ObjectProvider.class);
        when(provider.orderedStream()).thenReturn(Stream.of(beans));
        return provider;
    }

    @Test
    void customConversions_noConverterBeans_noOverride() {
        var config = new AerospikeDataConfigurationSupport() {};
        AerospikeCustomConversions result = config.customConversions(emptyProvider());

        assertThat(result.getCustomConverters()).isEmpty();
    }

    @Test
    void customConversions_noConverterBeans_withOverride() {
        var config = new AerospikeDataConfigurationSupport() {
            @Override
            protected List<Object> customConverters() {
                return List.of(IntToStringConverter.INSTANCE, StringToIntConverter.INSTANCE);
            }
        };

        AerospikeCustomConversions result = config.customConversions(emptyProvider());

        assertThat(result.getCustomConverters())
            .containsExactly(IntToStringConverter.INSTANCE, StringToIntConverter.INSTANCE);
    }

    @Test
    void customConversions_oneConverterBean() {
        var config = new AerospikeDataConfigurationSupport() {
            @Override
            protected List<Object> customConverters() {
                return List.of(IntToStringConverter.INSTANCE);
            }
        };

        var bean = new AerospikeCustomConverters(List.of(LongToStringConverter.INSTANCE));
        AerospikeCustomConversions result = config.customConversions(providerOf(bean));

        assertThat(result.getCustomConverters())
            .containsExactly(IntToStringConverter.INSTANCE, LongToStringConverter.INSTANCE);
    }

    @Test
    void customConversions_multipleConverterBeans() {
        var config = new AerospikeDataConfigurationSupport() {
            @Override
            protected List<Object> customConverters() {
                return List.of(IntToStringConverter.INSTANCE);
            }
        };

        var bean1 = new AerospikeCustomConverters(List.of(LongToStringConverter.INSTANCE));
        var bean2 = new AerospikeCustomConverters(List.of(StringToIntConverter.INSTANCE, StringToLongConverter.INSTANCE));
        AerospikeCustomConversions result = config.customConversions(providerOf(bean1, bean2));

        assertThat(result.getCustomConverters()).containsExactly(
            IntToStringConverter.INSTANCE,
            LongToStringConverter.INSTANCE,
            StringToIntConverter.INSTANCE,
            StringToLongConverter.INSTANCE
        );
    }
}
