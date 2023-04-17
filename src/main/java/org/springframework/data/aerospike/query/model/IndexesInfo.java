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
package org.springframework.data.aerospike.query.model;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexesInfo {

    private static final IndexesInfo EMPTY = new IndexesInfo(Collections.emptyMap());

    public final Map<IndexKey, Index> indexes;
    public final Set<IndexedField> indexedFields;

    private IndexesInfo(Map<IndexKey, Index> indexes) {
        this.indexes = Collections.unmodifiableMap(indexes);
        this.indexedFields = indexes.keySet().stream()
            .map(key -> new IndexedField(key.getNamespace(), key.getSet(), key.getField()))
            .distinct() // TODO: since we skip check on index type and index collection type in StatementBuilder
            .collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
    }

    public static IndexesInfo empty() {
        return EMPTY;
    }

    public static IndexesInfo of(Map<IndexKey, Index> cache) {
        return new IndexesInfo(cache);
    }
}
