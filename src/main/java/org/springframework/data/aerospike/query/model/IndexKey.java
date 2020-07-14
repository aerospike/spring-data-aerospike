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
package org.springframework.data.aerospike.query.model;

import com.aerospike.client.query.IndexType;

import java.util.Objects;

/**
 * @author Anastasiia Smirnova
 */
public class IndexKey {

	private final String namespace;
	private final String set;
	private final String field;
	private final IndexType type;

	public IndexKey(String namespace, String set, String field, IndexType type) {
		this.namespace = namespace;
		this.set = set;
		this.field = field;
		this.type = type;
	}

	public String getNamespace() {
		return namespace;
	}

	public String getSet() {
		return set;
	}

	public String getField() {
		return field;
	}

	public IndexType getType() {
		return type;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IndexKey indexKey = (IndexKey) o;
		return Objects.equals(namespace, indexKey.namespace) &&
				Objects.equals(set, indexKey.set) &&
				Objects.equals(field, indexKey.field) &&
				type == indexKey.type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(namespace, set, field, type);
	}
}
