package org.springframework.data.aerospike.query.cache;

import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexKey;
import org.springframework.data.aerospike.query.model.IndexedField;

import java.util.Optional;

public interface IndexesCache {

	Optional<Index> getIndex(IndexKey indexKey);

	boolean hasIndexFor(IndexedField indexedField);
}
