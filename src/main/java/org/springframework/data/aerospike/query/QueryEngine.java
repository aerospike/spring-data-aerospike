/*
 * Copyright 2012-2020 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.springframework.data.aerospike.query;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PredExp;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.IndexedField;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.function.Predicate;

import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.BETWEEN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.EQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GEO_WITHIN;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GT;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.GTEQ;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LT;
import static org.springframework.data.aerospike.query.Qualifier.FilterOperation.LTEQ;

/**
 * This class provides a multi-filter query engine that
 * augments the query capability in Aerospike.
 * To achieve this the class uses a UserDefined Function written in Lua to
 * provide the additional filtering. This UDF module packaged in the JAR and is automatically registered
 * with the cluster.
 *
 * @author peter
 */
public class QueryEngine {

	private static final EnumSet<Qualifier.FilterOperation> INDEXED_OPERATIONS = EnumSet.of(
			EQ, BETWEEN, GT, GTEQ, LT, LTEQ, GEO_WITHIN);

	private final AerospikeClient client;
	private final IndexesCache indexesCache;
	private final QueryPolicy queryPolicy;

	public enum Meta {
		KEY,
		TTL,
		EXPIRATION,
		GENERATION;

		@Override
		public String toString() {
			switch (this) {
				case KEY:
					return "__key";
				case EXPIRATION:
					return "__Expiration";
				case GENERATION:
					return "__generation";
				default:
					throw new IllegalArgumentException();
			}
		}
	}

	public QueryEngine(AerospikeClient client, QueryPolicy queryPolicy, IndexesCache indexesCache) {
		this.client = client;
		this.queryPolicy = queryPolicy;
		this.indexesCache = indexesCache;
	}

	/**
	 * Select records filtered by a Filter and Qualifiers
	 *
	 * @param namespace  Namespace to storing the data
	 * @param set        Set storing the data
	 * @param filter     Aerospike Filter to be used
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A KeyRecordIterator to iterate over the results
	 */
	public KeyRecordIterator select(String namespace, String set, Filter filter, Qualifier... qualifiers) {
		Statement stmt = new Statement();
		stmt.setNamespace(namespace);
		stmt.setSetName(set);
		if (filter != null)
			stmt.setFilter(filter);

		/*
		 * no filters
		 */
		if (qualifiers == null || qualifiers.length == 0) {
			RecordSet recordSet = this.client.query(queryPolicy, stmt);
			return new KeyRecordIterator(stmt.getNamespace(), recordSet);
		}
		/*
		 * singleton using primary key
		 */
		//TODO: if filter is provided together with KeyQualifier it is completely ignored (Anastasiia Smirnova)
		if (qualifiers.length == 1 && qualifiers[0] instanceof KeyQualifier) {
			KeyQualifier kq = (KeyQualifier) qualifiers[0];
			Key key = kq.makeKey(stmt.getNamespace(), stmt.getSetName());
			Record record = this.client.get(null, key, stmt.getBinNames());
			if (record == null) {
				return new KeyRecordIterator(stmt.getNamespace());
			} else {
				KeyRecord keyRecord = new KeyRecord(key, record);
				return new KeyRecordIterator(stmt.getNamespace(), keyRecord);
			}
		}
		/*
		 *  query with filters
		 */
		updateStatement(stmt, qualifiers, indexesCache::hasIndexFor);

		RecordSet rs = client.query(queryPolicy, stmt);
		return new KeyRecordIterator(stmt.getNamespace(), rs);

	}

	static void updateStatement(Statement stmt, Qualifier[] qualifiers,
								Predicate<IndexedField> indexPresent) {
		/*
		 *  query with filters
		 */
		for (int i = 0; i < qualifiers.length; i++) {
			Qualifier qualifier = qualifiers[i];

			if (qualifier == null) continue;
			if (qualifier.getOperation() == Qualifier.FilterOperation.AND) {
				for (Qualifier q : qualifier.getQualifiers()) {
					Filter filter = q == null ? null : q.asFilter();
					if (filter != null) {
						stmt.setFilter(filter);
						q.asFilter(true);
						break;
					}
				}
			} else if (isIndexedBin(stmt, qualifier, indexPresent)) {
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

		try {
			PredExp[] predexps;
			predexps = buildPredExp(qualifiers).toArray(new PredExp[0]);
			if (predexps.length > 0) {
				stmt.setPredExp(predexps);
				return;
			} else {
				throw new QualifierException("Failed to build Query");
			}
		} catch (PredExpException e) {
			throw new QualifierException(e.getMessage());
		}
	}

	private static boolean isIndexedBin(Statement stmt, Qualifier qualifier,
										Predicate<IndexedField> indexPresent) {
		if (null == qualifier.getField()) return false;

		return INDEXED_OPERATIONS.contains(qualifier.getOperation())
				&& indexPresent.test(new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getField()));
	}

	protected static List<PredExp> buildPredExp(Qualifier[] qualifiers) {
		List<PredExp> pes = new ArrayList<PredExp>();
		int qCount = 0;
		for (Qualifier q : qualifiers) {
			if (null != q && !q.queryAsFilter()) {
				List<PredExp> tpes = q.toPredExp();
				if (tpes.size() > 0) {
					pes.addAll(tpes);
					qCount++;
					q = null;
				}
			}
		}

		if (qCount > 1) pes.add(PredExp.and(qCount));
		return pes;
	}

}
