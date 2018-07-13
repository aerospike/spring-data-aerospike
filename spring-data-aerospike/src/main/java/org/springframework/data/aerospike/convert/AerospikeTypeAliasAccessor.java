package org.springframework.data.aerospike.convert;

import org.springframework.data.convert.TypeAliasAccessor;

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
	public Object readAliasFrom(Map<String, Object> source) {
		if (typeKey == null) {
			return null;
		}
		return source.get(typeKey);
	}

	@Override
	public void writeTypeTo(Map<String, Object> sink, Object alias) {
		if (typeKey != null) {
			sink.put(typeKey, alias);
		}
	}
}