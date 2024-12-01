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
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.Getter;
import lombok.Setter;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.springframework.data.aerospike.query.QualifierUtils.queryCriteriaIsNotNull;

/**
 * This class provides a multi-filter reactive query engine that augments the query capability in Aerospike.
 *
 * @author Sergii Karpenko
 * @author Anastasiia Smirnova
 */
public class ReactorQueryEngine {

    private final IAerospikeReactorClient client;
    @Getter
    private final StatementBuilder statementBuilder;
    @Getter
    private final FilterExpressionsBuilder filterExpressionsBuilder;
    private final AerospikeDataSettings dataSettings;
    /**
     * Scans can potentially slow down Aerospike server, so we are disabling them by default. If you still need to use
     * scans, set this property to true.
     */
    @Setter
    private boolean scansEnabled;
    @Setter
    @Getter
    private long queryMaxRecords;

    public ReactorQueryEngine(IAerospikeReactorClient client, StatementBuilder statementBuilder,
                              FilterExpressionsBuilder filterExpressionsBuilder, AerospikeDataSettings dataSettings) {
        this.client = client;
        this.statementBuilder = statementBuilder;
        this.filterExpressionsBuilder = filterExpressionsBuilder;
        this.dataSettings = dataSettings;
    }

    /**
     * Select records filtered by a Filter and Qualifiers
     *
     * @param namespace Namespace to storing the data
     * @param set       Set storing the data
     * @param query     {@link Query} for filtering results
     * @return A Flux<KeyRecord> to iterate over the results
     */
    public Flux<KeyRecord> select(String namespace, String set, @Nullable Query query) {
        return select(namespace, set, null, query);
    }

    /**
     * Select records filtered by a Filter and Qualifiers
     *
     * @param namespace Namespace to store the data
     * @param set       Set storing the data
     * @param binNames  Bin names to return from the query
     * @param query     {@link Query} for filtering results
     * @return A Flux<KeyRecord> to iterate over the results
     */
    public Flux<KeyRecord> select(String namespace, String set, String[] binNames, @Nullable Query query) {
        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteriaObject() : null;

        /*
         *  query with filters
         */
        if (query != null) {
            query.getCriteriaObject().setDataSettings(dataSettings);
        }
        Statement statement = statementBuilder.build(namespace, set, query, binNames);
        statement.setMaxRecords(queryMaxRecords);
        QueryPolicy localQueryPolicy = getQueryPolicy(qualifier, true);

        if (!scansEnabled && statement.getFilter() == null) {
            return Flux.error(new IllegalStateException(QueryEngine.SCANS_DISABLED_MESSAGE));
        }

        return client.query(localQueryPolicy, statement);
    }

    /**
     * Select records filtered by a query to be counted
     *
     * @param namespace Namespace to store the data
     * @param set       Set storing the data
     * @param query     {@link Query} for filtering results
     * @return A Flux<KeyRecord> for counting
     */
    public Flux<KeyRecord> selectForCount(String namespace, String set, @Nullable Query query) {
        Statement statement = statementBuilder.build(namespace, set, query);
        statement.setMaxRecords(queryMaxRecords);
        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteriaObject() : null;
        QueryPolicy localQueryPolicy = getQueryPolicy(qualifier, false);

        if (!scansEnabled && statement.getFilter() == null) {
            return Flux.error(new IllegalStateException(QueryEngine.SCANS_DISABLED_MESSAGE));
        }

        return client.query(localQueryPolicy, statement);
    }

    private QueryPolicy getQueryPolicy(Qualifier qualifier, boolean includeBins) {
        QueryPolicy queryPolicy = new QueryPolicy(client.getQueryPolicyDefault());
        queryPolicy.filterExp = filterExpressionsBuilder.build(qualifier);
        queryPolicy.includeBinData = includeBins;
        return queryPolicy;
    }

    @SuppressWarnings("SameParameterValue")
    private Mono<KeyRecord> getRecord(Policy policy, Key key, String[] binNames) {
        if (binNames == null || binNames.length == 0) {
            return client.get(policy, key);
        }
        return client.get(policy, key, binNames);
    }
}
