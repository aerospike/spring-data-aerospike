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

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

        return converters;
    }

    public enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
        INSTANCE;

        @Override
        public String convert(BigDecimal source) {
            return source.toString();
        }
    }

    public enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
        INSTANCE;

        @Override
        public BigDecimal convert(String source) {
            return StringUtils.hasText(source) ? new BigDecimal(source) : null;
        }
    }

    public enum BigIntegerToStringConverter implements Converter<BigInteger, String> {
        INSTANCE;

        @Override
        public String convert(BigInteger source) {
            return source.toString();
        }
    }

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
}
