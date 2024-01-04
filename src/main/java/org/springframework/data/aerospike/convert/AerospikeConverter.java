/*
 * Copyright 2011-2018 the original author or authors.
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

import org.springframework.core.convert.ConversionService;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.convert.EntityConverter;

/**
 * Interface of an Aerospike-specific {@link EntityConverter}.
 *
 * @author Oliver Gierke
 */
public interface AerospikeConverter extends AerospikeReader<Object>, AerospikeWriter<Object> {

    /**
     * Access Aerospike-specific conversion service.
     *
     * @return the underlying {@link ConversionService} used by the converter
     */
    ConversionService getConversionService();

    /**
     * Access Aerospike-specific data settings.
     *
     * @return the underlying {@link AerospikeDataSettings} used by the converter
     */
    AerospikeDataSettings getAerospikeSettings();
}
