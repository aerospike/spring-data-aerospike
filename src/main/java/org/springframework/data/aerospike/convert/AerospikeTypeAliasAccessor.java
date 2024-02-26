/*
 * Copyright 2012-2018 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.convert;

import org.springframework.data.convert.TypeAliasAccessor;
import org.springframework.data.mapping.Alias;

import java.util.Map;

import static org.springframework.data.aerospike.convert.AerospikeConverter.CLASS_KEY;

public class AerospikeTypeAliasAccessor implements TypeAliasAccessor<Map<String, Object>> {

    private final String classKey;

    public AerospikeTypeAliasAccessor(String classKey) {
        this.classKey = classKey;
    }

    public AerospikeTypeAliasAccessor() {
        this.classKey = CLASS_KEY;
    }

    @Override
    public Alias readAliasFrom(Map<String, Object> source) {
        if (classKey == null) {
            return Alias.NONE;
        }
        return Alias.ofNullable(source.get(classKey));
    }

    @Override
    public void writeTypeTo(Map<String, Object> sink, Object alias) {
        if (classKey != null) {
            sink.put(classKey, alias);
        }
    }
}
