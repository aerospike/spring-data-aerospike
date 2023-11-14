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

import java.util.List;
import java.util.Optional;

public interface IndexesCache {

    /**
     * Get index from the indexes cache by providing an index key.
     *
     * @param indexKey Index key to search by.
     * @return Optional {@link Index}
     */
    Optional<Index> getIndex(IndexKey indexKey);

    /**
     * Get all indexes for a given indexed field object.
     *
     * @param indexedField Indexed field to search by.
     * @return List of indexes matching the indexed field properties.
     */
    List<Index> getAllIndexesForField(IndexedField indexedField);

    /**
     * Boolean indication of whether an index exists for the given indexed field
     *
     * @param indexedField Indexed field to search by
     * @return True if there is an index for the given indexed field.
     */
    boolean hasIndexFor(IndexedField indexedField);
}
