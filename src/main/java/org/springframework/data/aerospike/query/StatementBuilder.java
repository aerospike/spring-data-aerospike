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
package org.springframework.data.aerospike.query;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.Statement;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.IndexedField;

/**
 * @author peter
 * @author Anastasiia Smirnova
 */
public class StatementBuilder {

	private final IndexesCache indexesCache;

	public StatementBuilder(IndexesCache indexesCache) {
		this.indexesCache = indexesCache;
	}

	public Statement build(String namespace, String set, Filter filter, Qualifier[] qualifiers) {
		return build(namespace, set, filter, qualifiers, null);
	}

	public Statement build(String namespace, String set, Filter filter, Qualifier[] qualifiers, String[] binNames) {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (binNames != null && binNames.length != 0) {
			stmt.setBinNames(binNames);
		}
		if (filter != null) {
			stmt.setFilter(filter);
		}
		if (qualifiers != null && qualifiers.length != 0) {
			updateStatement(stmt, qualifiers);
		}
		return stmt;
	}

	private void updateStatement(Statement stmt, Qualifier[] qualifiers) {
		/*
		 *  query with filters
		 */
		for (int i = 0; i < qualifiers.length; i++) {
			Qualifier qualifier = qualifiers[i];

			if (qualifier == null) continue;
			if (qualifier.getOperation() == FilterOperation.AND) {
				for (Qualifier q : qualifier.getQualifiers()) {
					if(q != null && isIndexedBin(stmt, q)) {
						Filter filter = q.asFilter();
						if (filter != null) {
							stmt.setFilter(filter);
							q.asFilter(true);
							break;
						}
					}
				}
			} else if (isIndexedBin(stmt, qualifier)) {
				Filter filter = qualifier.asFilter();
				if (filter != null) {
					stmt.setFilter(filter);
					qualifier.asFilter(true);
					qualifiers[i] = null;
					/* If this was the only qualifier, we do not need to do anymore work, just return
					 * the query iterator.
					 */
					if (qualifiers.length == 1) {
						return;
					}
					break;
				}
			}
		}
	}

	private boolean isIndexedBin(Statement stmt, Qualifier qualifier) {
		if (qualifier.getField() == null) return false;

		//TODO: skips check on index-type and index-collection-type
		return indexesCache.hasIndexFor(new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getField()));
	}
}
