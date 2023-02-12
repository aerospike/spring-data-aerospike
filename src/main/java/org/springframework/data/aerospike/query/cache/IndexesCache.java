/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.aerospike.query.cache;

import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.model.IndexedField;

import java.util.Optional;

public interface IndexesCache {

    /**
     * @param indexKey to search by
     * @return Optional {@link Index}
     */
    Optional<Index> getIndex(IndexKey indexKey);

    /**
     * @param indexedField to search by
     * @return true if there is an index for the given indexed field
     */
    boolean hasIndexFor(IndexedField indexedField);
}
