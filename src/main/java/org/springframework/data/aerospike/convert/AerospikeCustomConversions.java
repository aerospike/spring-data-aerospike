/*
 * Copyright 2020 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import lombok.Getter;
import org.springframework.data.aerospike.mapping.AerospikeSimpleTypes;
import org.springframework.data.convert.CustomConversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Value object to capture custom conversion.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @author Anastasiia Smirnova
 * @see CustomConversions
 * @see org.springframework.data.mapping.model.SimpleTypeHolder
 * @see AerospikeSimpleTypes
 */
public class AerospikeCustomConversions extends CustomConversions {

    private static final StoreConversions STORE_CONVERSIONS;
    private static final List<Object> STORE_CONVERTERS;
    @Getter
    private final List<Object> customConverters;

    static {
        List<Object> converters = new ArrayList<>();

        converters.addAll(DateConverters.getConvertersToRegister());
        converters.addAll(AerospikeConverters.getConvertersToRegister());

        STORE_CONVERTERS = Collections.unmodifiableList(converters);
        STORE_CONVERSIONS = StoreConversions.of(AerospikeSimpleTypes.HOLDER, STORE_CONVERTERS);
    }

    /**
     * Create a new instance with a given list of converters
     *
     * @param converters the list of custom converters
     */
    public AerospikeCustomConversions(final List<Object> converters) {
        super(STORE_CONVERSIONS, converters);
        this.customConverters = converters;
    }
}
