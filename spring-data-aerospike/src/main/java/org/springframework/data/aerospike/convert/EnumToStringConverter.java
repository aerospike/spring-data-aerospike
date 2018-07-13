package org.springframework.data.aerospike.convert;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.convert.WritingConverter;

@WritingConverter
public class EnumToStringConverter implements Converter<Enum<?>, String> {

	@Override
	public String convert(Enum<?> source) {
		return source.name();
	}

}