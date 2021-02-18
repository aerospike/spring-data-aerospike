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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.core.convert.converter.Converter;

import com.aerospike.client.Bin;
import com.aerospike.client.Value;
import com.aerospike.client.Value.GeoJSONValue;
import org.springframework.data.convert.ReadingConverter;
import org.springframework.data.convert.WritingConverter;
import org.springframework.util.StringUtils;

/**
 * Wrapper class to contain useful converters 
 * 
 * @author Peter Milne
 * @author Anastasiia Smirnova
 */
abstract class AerospikeConverters {

	private AerospikeConverters() {}

	static Collection<Object> getConvertersToRegister() {

		List<Object> converters = new ArrayList<>();

		converters.add(BigDecimalToStringConverter.INSTANCE);
		converters.add(StringToBigDecimalConverter.INSTANCE);
		converters.add(LongToBooleanConverter.INSTANCE);
		converters.add(EnumToStringConverter.INSTANCE);

		return converters;
	}

	public enum BigDecimalToStringConverter implements Converter<BigDecimal, String> {
		INSTANCE;

		public String convert(BigDecimal source) {
			return source.toString();
		}
	}

	public enum StringToBigDecimalConverter implements Converter<String, BigDecimal> {
		INSTANCE;

		public BigDecimal convert(String source) {
			return StringUtils.hasText(source) ? new BigDecimal(source) : null;
		}
	}

	/**
	 * @author Peter Milne
	 * @author Jean Mercier
	 */
	@ReadingConverter
	public enum LongToBooleanConverter implements Converter<Long, Boolean> {
		INSTANCE;

		@Override
		public Boolean convert(Long source) {
			return source != 0L;
		}

	}

	/**
	 * @author Anastasiia Smirnova
	 */
	@WritingConverter
	public enum EnumToStringConverter implements Converter<Enum<?>, String> {
		INSTANCE;

		@Override
		public String convert(Enum<?> source) {
			return source.name();
		}

	}

	public enum LongToValueConverter implements Converter<Long, Value> {
		INSTANCE;

		@Override
		public Value convert(Long source) {
			return Value.get(source);
		}
	}

	public enum BinToLongConverter implements Converter<Bin, Long> {
		INSTANCE;

		@Override
		public Long convert(Bin source) {
			return source.value.toLong();
		}
	}

	public enum StringToValueConverter implements Converter<String, Value> {
		INSTANCE;

		@Override
		public Value convert(String source) {
			return Value.get(source);
		}
	}

	public enum BinToStringConverter implements Converter<Bin, String> {
		INSTANCE;

		@Override
		public String convert(Bin source) {
			return source.value.toString();
		}
	}

	public enum ListToValueConverter implements Converter<List<?>, Value> {
		INSTANCE;

		@Override
		public Value convert(List<?> source) {
			return Value.get(source);
		}
	}

	public enum MapToValueConverter implements Converter<Map<?, ?>, Value> {
		INSTANCE;

		@Override
		public Value convert(Map<?, ?> source) {
			return Value.get(source);
		}
	}

	public enum BytesToValueConverter implements Converter<Byte[], Value> {
		INSTANCE;

		@Override
		public Value convert(Byte[] source) {
			return Value.get(source);
		}
	}
	
	public enum StringToAerospikeGeoJSONValueConverter implements Converter<String, GeoJSONValue> {
		INSTANCE;
		
		/* (non-Javadoc)
		 * @see org.springframework.core.convert.converter.Converter#convert(java.lang.Object)
		 */
		@Override
		public GeoJSONValue convert(String source) {
			return new GeoJSONValue(source);
		}
	}

}
