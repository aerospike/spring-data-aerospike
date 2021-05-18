/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.Key;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.util.List;
import java.util.Map;

@Value
@Builder
@Getter
class EntitiesKeys {

    Class<?>[] entityClasses;
    Key[] keys;

    public static EntitiesKeys of(Map<Class<?>, List<Key>> entitiesKeys) {
        Class<?>[] entityClasses = entitiesKeys.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream().map(item -> entry.getKey()))
                .toArray(Class<?>[]::new);

        Key[] keys = entitiesKeys.entrySet().stream()
                .flatMap(entry -> entry.getValue().stream())
                .toArray(Key[]::new);

        return EntitiesKeys.builder()
                .entityClasses(entityClasses)
                .keys(keys)
                .build();
    }
}
