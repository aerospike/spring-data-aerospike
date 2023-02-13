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

public class AerospikeTypeAliasAccessor implements TypeAliasAccessor<Map<String, Object>> {

    private static final String TYPE_KEY = "@_class";
    private final String typeKey;

    public AerospikeTypeAliasAccessor(String typeKey) {
        this.typeKey = typeKey;
    }

    public AerospikeTypeAliasAccessor() {
        this.typeKey = TYPE_KEY;
    }

    @Override
    public Alias readAliasFrom(Map<String, Object> source) {
        return Alias.ofNullable(source.get(typeKey));
    }

    @Override
    public void writeTypeTo(Map<String, Object> sink, Object alias) {
        if (typeKey != null) {
            sink.put(typeKey, alias);
        }
    }
}
