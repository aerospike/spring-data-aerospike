/*
 * Copyright 2015-2020 the original author or authors.
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

import static java.util.Arrays.asList;

import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.EntityInstantiators;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeMapper;

/**
 * An implementation of {@link AerospikeConverter} to read domain objects from {@link AerospikeReadData} and write domain
 * objects into {@link AerospikeWriteData}.
 *
 * @author Oliver Gierke
 */
public class MappingAerospikeConverter implements InitializingBean, AerospikeConverter {

	private final CustomConversions conversions;
	private final GenericConversionService conversionService;
	private final MappingAerospikeReadConverter readConverter;
	private final MappingAerospikeWriteConverter writeConverter;

	/**
	 * Creates a new {@link MappingAerospikeConverter}.
	 */
	public MappingAerospikeConverter(AerospikeMappingContext mappingContext, CustomConversions conversions,
									 AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor) {
		this.conversions = conversions;
		this.conversionService = new DefaultConversionService();

		EntityInstantiators entityInstantiators = new EntityInstantiators();
		TypeMapper<Map<String, Object>> typeMapper = new DefaultTypeMapper<>(aerospikeTypeAliasAccessor,
				mappingContext, asList(new SimpleTypeInformationMapper()));

		this.writeConverter = new MappingAerospikeWriteConverter(typeMapper, mappingContext, conversions, conversionService);
		this.readConverter = new MappingAerospikeReadConverter(entityInstantiators, aerospikeTypeAliasAccessor, typeMapper, mappingContext, conversions, conversionService);
	}

	@Override
	public void afterPropertiesSet() {
		conversions.registerConvertersIn(conversionService);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getConversionService()
	 */
	@Override
	public ConversionService getConversionService() {
		return conversionService;
	}


	@Override
	public <R> R read(Class<R> type, final AerospikeReadData data) {
		return readConverter.read(type, data);
	}

	@Override
	public void write(Object source, AerospikeWriteData sink) {
		writeConverter.write(source, sink);
	}

}
