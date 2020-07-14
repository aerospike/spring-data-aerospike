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
				.collect(Collectors.collectingAndThen(Collectors.toSet(), Collections::unmodifiableSet));
	}

	public static IndexesInfo empty() {
		return EMPTY;
	}

	public static IndexesInfo of(Map<IndexKey, Index> cache) {
		return new IndexesInfo(cache);
	}
}
