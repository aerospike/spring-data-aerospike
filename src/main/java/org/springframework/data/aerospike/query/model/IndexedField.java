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

import java.util.Objects;

/**
 * @author Anastasiia Smirnova
 */
public class IndexedField {

	private final String namespace;
	private final String set;
	private final String field;

	public IndexedField(String namespace, String set, String field) {
		this.namespace = namespace;
		this.set = set;
		this.field = field;
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

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		IndexedField that = (IndexedField) o;
		return Objects.equals(namespace, that.namespace) &&
				Objects.equals(set, that.set) &&
				Objects.equals(field, that.field);
	}

	@Override
	public int hashCode() {
		return Objects.hash(namespace, set, field);
	}
}
