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
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;

/**
 * This class provides a multi-filter query engine that
 * augments the query capability in Aerospike.
 *
 * @author peter
 * @author Anastasiia Smirnova
 */
public class QueryEngine {

	private final AerospikeClient client;
	private final StatementBuilder statementBuilder;
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

	public QueryEngine(AerospikeClient client, StatementBuilder statementBuilder,
					   QueryPolicy queryPolicy) {
		this.client = client;
		this.statementBuilder = statementBuilder;
		this.queryPolicy = queryPolicy;
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
		/*
		 * singleton using primary key
		 */
		//TODO: if filter is provided together with KeyQualifier it is completely ignored (Anastasiia Smirnova)
		if (qualifiers != null && qualifiers.length == 1 && qualifiers[0] instanceof KeyQualifier) {
			KeyQualifier kq = (KeyQualifier) qualifiers[0];
			Key key = kq.makeKey(namespace, set);
			Record record = this.client.get(null, key);
			if (record == null) {
				return new KeyRecordIterator(namespace);
			} else {
				KeyRecord keyRecord = new KeyRecord(key, record);
				return new KeyRecordIterator(namespace, keyRecord);
			}
		}

		/*
		 *  query with filters
		 */
		Statement statement = statementBuilder.build(namespace, set, filter, qualifiers);
		RecordSet rs = client.query(queryPolicy, statement);
		return new KeyRecordIterator(namespace, rs);
	}

}
