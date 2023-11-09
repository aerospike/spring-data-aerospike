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

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import lombok.Getter;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.lang.Nullable;

import static org.springframework.data.aerospike.query.QualifierUtils.queryCriteriaIsNotNull;

/**
 * This class provides a multi-filter query engine that augments the query capability in Aerospike.
 *
 * @author peter
 * @author Anastasiia Smirnova
 */
public class QueryEngine {

    public static final String SCANS_DISABLED_MESSAGE =
        "Query without a filter will initiate a scan. Since scans are potentially dangerous operations, they are " +
            "disabled by default in spring-data-aerospike. " +
            "If you still need to use them, enable them via `scansEnabled` property in `org.springframework.data" +
            ".aerospike.config.AerospikeDataSettings`.";
    private final IAerospikeClient client;
    private final StatementBuilder statementBuilder;
    @Getter
    private final FilterExpressionsBuilder filterExpressionsBuilder;
    @Getter
    private final QueryPolicy queryPolicy;
    /**
     * Scans can potentially slow down Aerospike server, so we are disabling them by default. If you still need to use
     * scans, set this property to true.
     */
    private boolean scansEnabled = false;

    public QueryEngine(IAerospikeClient client, StatementBuilder statementBuilder,
                       FilterExpressionsBuilder filterExpressionsBuilder, QueryPolicy queryPolicy) {
        this.client = client;
        this.statementBuilder = statementBuilder;
        this.filterExpressionsBuilder = filterExpressionsBuilder;
        this.queryPolicy = queryPolicy;
    }

    /**
     * Select records filtered by a query
     *
     * @param namespace Namespace to storing the data
     * @param set       Set storing the data
     * @param query     {@link Query} for filtering results
     * @return A KeyRecordIterator to iterate over the results
     */
    public KeyRecordIterator select(String namespace, String set, @Nullable Query query) {
        return select(namespace, set, null, query);
    }

    /**
     * Select records filtered by a query
     *
     * @param namespace Namespace to storing the data
     * @param set       Set storing the data
     * @param binNames  Bin names to return from the query
     * @param query     {@link Query} for filtering results
     * @return A KeyRecordIterator to iterate over the results
     */
    public KeyRecordIterator select(String namespace, String set, String[] binNames, @Nullable Query query) {
        Qualifier qualifier = queryCriteriaIsNotNull(query) ? query.getCriteria().getCriteriaObject() : null;
        /*
         * singleton using primary key
         */
        // KeyQualifier is deprecated and marked for removal
        if (qualifier instanceof KeyQualifier kq) {
            Key key = kq.makeKey(namespace, set);
            Record record = getRecord(null, key, binNames);
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
        Statement statement = statementBuilder.build(namespace, set, query, binNames);
        QueryPolicy localQueryPolicy = new QueryPolicy(queryPolicy);
        localQueryPolicy.filterExp = filterExpressionsBuilder.build(query);

        if (!scansEnabled && statement.getFilter() == null) {
            throw new IllegalStateException(SCANS_DISABLED_MESSAGE);
        }

        RecordSet rs = client.query(localQueryPolicy, statement);
        return new KeyRecordIterator(namespace, rs);
    }

    @SuppressWarnings("SameParameterValue")
    private Record getRecord(Policy policy, Key key, String[] binNames) {
        if (binNames == null || binNames.length == 0) {
            return client.get(policy, key);
        }
        return client.get(policy, key, binNames);
    }

    public void setScansEnabled(boolean scansEnabled) {
        this.scansEnabled = scansEnabled;
    }

    public enum Meta {
        KEY,
        TTL,
        EXPIRATION,
        GENERATION;

        @Override
        public String toString() {
            return switch (this) {
                case KEY -> "__key";
                case EXPIRATION -> "__Expiration";
                case GENERATION -> "__generation";
                default -> throw new IllegalArgumentException();
            };
        }
    }
}
