/*
 * Copyright 2012-2019 Aerospike, Inc.
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

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * This class provides a multi-filter reactive query engine that
 * augments the query capability in Aerospike.
 *
 * @author Sergii Karpenko
 * @author Anastasiia Smirnova
 */
public class ReactorQueryEngine {

	/**
	 * Scans can potentially slow down Aerospike server, so we are disabling them by default.
	 * If you still need to use scans, set this property to true.
	 */
	private boolean scansEnabled = false;

	private final IAerospikeReactorClient client;
	private final StatementBuilder statementBuilder;
	private final FilterExpressionsBuilder filterExpressionsBuilder;
	private final QueryPolicy queryPolicy;

	public ReactorQueryEngine(IAerospikeReactorClient client, StatementBuilder statementBuilder,
							  FilterExpressionsBuilder filterExpressionsBuilder, QueryPolicy queryPolicy) {
		this.client = client;
		this.statementBuilder = statementBuilder;
		this.filterExpressionsBuilder = filterExpressionsBuilder;
		this.queryPolicy = queryPolicy;
	}

	/**
	 * Select records filtered by a Filter and Qualifiers
	 *
	 * @param namespace  Namespace to storing the data
	 * @param set        Set storing the data
	 * @param filter     Aerospike Filter to be used
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A Flux<KeyRecord> to iterate over the results
	 */
	public Flux<KeyRecord> select(String namespace, String set, Filter filter, Qualifier... qualifiers) {
		return select(namespace, set, null, filter, qualifiers);
	}

	/**
	 * Select records filtered by a Filter and Qualifiers
	 *
	 * @param namespace  Namespace to storing the data
	 * @param set        Set storing the data
	 * @param binNames   Bin names to return from the query
	 * @param filter     Aerospike Filter to be used
	 * @param qualifiers Zero or more Qualifiers for the update query
	 * @return A Flux<KeyRecord> to iterate over the results
	 */
	public Flux<KeyRecord> select(String namespace, String set, String[] binNames, Filter filter, Qualifier... qualifiers) {
		/*
		 * singleton using primary key
		 */
		//TODO: if filter is provided together with KeyQualifier it is completely ignored (Anastasiia Smirnova)
		if (qualifiers != null && qualifiers.length == 1 && qualifiers[0] instanceof KeyQualifier) {
			KeyQualifier kq = (KeyQualifier) qualifiers[0];
			Key key = kq.makeKey(namespace, set);
			return Flux.from(getRecord(null, key, binNames))
					.filter(keyRecord -> Objects.nonNull(keyRecord.record));
		}

		/*
		 *  query with filters
		 */
		Statement statement = statementBuilder.build(namespace, set, filter, qualifiers, binNames);
		QueryPolicy localQueryPolicy = new QueryPolicy(queryPolicy);
		localQueryPolicy.filterExp = filterExpressionsBuilder.build(qualifiers);
		if (!scansEnabled && statement.getFilter() == null) {
			return Flux.error(new IllegalStateException(QueryEngine.SCANS_DISABLED_MESSAGE));
		}
		return client.query(localQueryPolicy, statement);
	}

	private Mono<KeyRecord> getRecord(Policy policy, Key key, String[] binNames) {
		if (binNames == null || binNames.length == 0) {
			return client.get(policy, key);
		}
		return client.get(policy, key, binNames);
	}

	public void setScansEnabled(boolean scansEnabled) {
		this.scansEnabled = scansEnabled;
	}
}
