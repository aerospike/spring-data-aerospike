/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.aerospike.query.cache;

import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.data.aerospike.query.model.IndexesInfo;

import java.util.Optional;

/**
 * @author Anastasiia Smirnova
 */
public class IndexesCacheHolder implements IndexesCache, IndexesCacheUpdater {

	private volatile IndexesInfo cache = IndexesInfo.empty();

	@Override
	public Optional<Index> getIndex(IndexKey indexKey) {
		return Optional.ofNullable(cache.indexes.get(indexKey));
	}

	@Override
	public boolean hasIndexFor(IndexedField indexedField) {
		return cache.indexedFields.contains(indexedField);
	}

	@Override
	public void update(IndexesInfo cache) {
		this.cache = cache;
	}
}
