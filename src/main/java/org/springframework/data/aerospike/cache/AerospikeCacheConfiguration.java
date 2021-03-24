/*
 * Copyright 2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.cache;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Default set is null meaning write directly to the namespace.
 * Default expiration is 0 meaning use the server's default namespace configuration variable "default-ttl".
 */
@Getter
@AllArgsConstructor
public class AerospikeCacheConfiguration {
    private final String namespace;
    private final String set;
    private final int expirationInSeconds;

    public AerospikeCacheConfiguration (String namespace) {
        this(namespace, null, 0);
    }

    public AerospikeCacheConfiguration (String namespace, String set) {
        this(namespace, set, 0);
    }

    public AerospikeCacheConfiguration (String namespace, int expirationInSeconds) {
        this(namespace, null, expirationInSeconds);
    }
}
