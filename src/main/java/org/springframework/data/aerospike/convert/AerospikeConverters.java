/*
 * Copyright 2011-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import org.springframework.core.convert.ConversionFailedException;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Wrapper class to contain useful converters
 *
 * @author Peter Milne
 * @author Anastasiia Smirnova
 */
abstract class AerospikeConverters {

    private AerospikeConverters() {
    }

    static Collection<Object> getConvertersToRegister() {
        List<Object> converters = new ArrayList<>();

        converters.add(BigDecimalToStringConverter.INSTANCE);
        converters.add(StringToBigDecimalConverter.INSTANCE);
        converters.add(BigIntegerToStringConverter.INSTANCE);
        converters.add(StringToBigIntegerConverter.INSTANCE);
        converters.add(LongToBooleanConverter.INSTANCE);
        converters.add(EnumToStringConverter.INSTANCE);
        converters.add(URLToStringConverter.INSTANCE);
        converters.add(StringToURLConverter.INSTANCE);
        converters.add(AtomicIntegerToIntegerConverter.INSTANCE);
        converters.add(IntegerToAtomicIntegerConverter.INSTANCE);
        converters.add(AtomicLongToLongConverter.INSTANCE);
        converters.add(LongToAtomicLongConverter.INSTANCE);
        converters.add(CurrencyToStringConverter.INSTANCE);
        converters.add(StringToCurrencyConverter.INSTANCE);
        converters.add(UuidToStringConverter.INSTANCE);
        converters.add(StringToUuidConverter.INSTANCE);

        return converters;
    }

    @WritingConverter
    public enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
        INSTANCE;

        @Override
        public String convert(BigDecimal source) {
            return source.toString();
        }
    }

    @ReadingConverter
    public enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
        INSTANCE;

        @Override
        public BigDecimal convert(String source) {
            return StringUtils.hasText(source) ? new BigDecimal(source) : null;
        }
    }

    @WritingConverter
    public enum BigIntegerToStringConverter implements Converter<BigInteger, String> {
        INSTANCE;

        @Override
        public String convert(BigInteger source) {
            return source.toString();
        }
    }

    @ReadingConverter
    public enum StringToBigIntegerConverter implements Converter<String, BigInteger> {
        INSTANCE;

        @Override
        public BigInteger convert(String source) {
            return new BigInteger(source);
        }
    }

    @ReadingConverter
    public enum LongToBooleanConverter implements Converter<Long, Boolean> {
        INSTANCE;

        @Override
        public Boolean convert(Long source) {
            return source != 0L;
        }
    }

    @WritingConverter
    public enum EnumToStringConverter implements Converter<Enum<?>, String> {
        INSTANCE;

        @Override
        public String convert(Enum<?> source) {
            return source.name();
        }
    }

    @WritingConverter
    enum URLToStringConverter implements Converter<URL, String> {
        INSTANCE;

        public String convert(URL source) {
            return source.toString();
        }
    }

    @ReadingConverter
    enum StringToURLConverter implements Converter<String, URL> {
        INSTANCE;

        private static final TypeDescriptor source = TypeDescriptor.valueOf(String.class);
        private static final TypeDescriptor target = TypeDescriptor.valueOf(URL.class);

        public URL convert(String source) {
            try {
                return new URL(source);
            } catch (MalformedURLException e) {
                throw new ConversionFailedException(StringToURLConverter.source, target, source, e);
            }
        }
    }

    @WritingConverter
    enum AtomicIntegerToIntegerConverter implements Converter<AtomicInteger, Integer> {
        INSTANCE;

        @Override
        public Integer convert(AtomicInteger source) {
            return NumberUtils.convertNumberToTargetClass(source, Integer.class);
        }
    }

    @ReadingConverter
    enum IntegerToAtomicIntegerConverter implements Converter<Integer, AtomicInteger> {
        INSTANCE;

        @Override
        public AtomicInteger convert(Integer source) {
            return new AtomicInteger(source);
        }
    }

    @WritingConverter
    enum AtomicLongToLongConverter implements Converter<AtomicLong, Long> {
        INSTANCE;

        @Override
        public Long convert(AtomicLong source) {
            return NumberUtils.convertNumberToTargetClass(source, Long.class);
        }
    }

    @ReadingConverter
    enum LongToAtomicLongConverter implements Converter<Long, AtomicLong> {
        INSTANCE;

        @Override
        public AtomicLong convert(Long source) {
            return new AtomicLong(source);
        }
    }

    @WritingConverter
    enum CurrencyToStringConverter implements Converter<Currency, String> {

        INSTANCE;

        @Override
        public String convert(Currency source) {
            return source.getCurrencyCode();
        }
    }

    @ReadingConverter
    enum StringToCurrencyConverter implements Converter<String, Currency> {

        INSTANCE;

        @Override
        public Currency convert(String source) {
            return StringUtils.hasText(source) ? Currency.getInstance(source) : null;
        }
    }

    @WritingConverter
    public enum UuidToStringConverter implements Converter<UUID, String> {
        INSTANCE;

        @Override
        public String convert(UUID source) {
            return source == null ? null : source.toString();
        }
    }

    @ReadingConverter
    public enum StringToUuidConverter implements Converter<String, UUID> {
        INSTANCE;

        @Override
        public UUID convert(String source) {
            return source == null ? null : UUID.fromString(source);
        }
    }
}
