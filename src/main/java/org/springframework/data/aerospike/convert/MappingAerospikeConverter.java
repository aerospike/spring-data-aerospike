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

import lombok.Getter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.convert.DefaultTypeMapper;
import org.springframework.data.convert.SimpleTypeInformationMapper;
import org.springframework.data.convert.TypeMapper;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.util.TypeInformation;

import java.util.List;
import java.util.Map;

/**
 * An implementation of {@link AerospikeConverter} to read domain objects from {@link AerospikeReadData} and write
 * domain objects into {@link AerospikeWriteData}.
 *
 * @author Oliver Gierke
 */
public class MappingAerospikeConverter implements InitializingBean, AerospikeConverter {

    private final CustomConversions conversions;
    @Getter
    private final GenericConversionService conversionService;
    @Getter
    private final AerospikeDataSettings aerospikeDataSettings;
    private final MappingAerospikeReadConverter readConverter;
    private final MappingAerospikeWriteConverter writeConverter;

    /**
     * Creates a new {@link MappingAerospikeConverter}.
     */
    public MappingAerospikeConverter(AerospikeMappingContext mappingContext, CustomConversions conversions,
                                     AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor,
                                     AerospikeDataSettings settings) {
        this.conversions = conversions;
        this.conversionService = new DefaultConversionService();
        this.aerospikeDataSettings = settings;

        EntityInstantiators entityInstantiators = new EntityInstantiators();
        TypeMapper<Map<String, Object>> typeMapper = new DefaultTypeMapper<>(aerospikeTypeAliasAccessor,
            mappingContext, List.of(new SimpleTypeInformationMapper()));

        this.writeConverter =
            new MappingAerospikeWriteConverter(typeMapper, mappingContext, conversions, conversionService,
                settings);
        this.readConverter = new MappingAerospikeReadConverter(entityInstantiators, aerospikeTypeAliasAccessor,
            typeMapper, mappingContext, conversions, conversionService);
    }

    @Override
    public void afterPropertiesSet() {
        conversions.registerConvertersIn(conversionService);
    }

    @Override
    public <R> R read(Class<R> type, final AerospikeReadData data) {
        return readConverter.read(type, data);
    }

    @Override
    public void write(Object source, AerospikeWriteData sink) {
        writeConverter.write(source, sink);
    }

    public Object toWritableValue(Object source, TypeInformation<?> type) {
        return writeConverter.getValueToWrite(source, type);
    }

    public CustomConversions getCustomConversions() {
        return this.conversions;
    }
}
