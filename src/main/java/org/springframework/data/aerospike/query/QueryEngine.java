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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.lang.Nullable;

import java.util.List;

import static com.aerospike.client.ResultCode.INDEX_GENERIC;
import static com.aerospike.client.ResultCode.INDEX_MAXCOUNT;
import static com.aerospike.client.ResultCode.INDEX_NAME_MAXLEN;
import static com.aerospike.client.ResultCode.INDEX_NOTFOUND;
import static com.aerospike.client.ResultCode.INDEX_NOTREADABLE;
import static com.aerospike.client.ResultCode.INDEX_OOM;
import static org.springframework.data.aerospike.query.QualifierUtils.isQueryCriteriaNotNull;

/**
 * This class provides a multi-filter query engine that augments the query capability in Aerospike.
 *
 * @author peter
 * @author Anastasiia Smirnova
 */
@Slf4j
public class QueryEngine {

    public static final String SCANS_DISABLED_MESSAGE =
        "Query without a secondary index filter will initiate a scan. Since scans are potentially dangerous " +
            "operations, they are disabled by default in spring-data-aerospike. " +
            "If you still need to use them, enable them via `scans-enabled` property.";
    public static final List<Integer> SEC_INDEX_ERROR_RESULT_CODES = List.of(
        INDEX_NOTFOUND, INDEX_OOM, INDEX_NOTREADABLE, INDEX_GENERIC, INDEX_NAME_MAXLEN, INDEX_MAXCOUNT);
    private final IAerospikeClient client;
    @Getter
    private final QueryContextBuilder queryContextBuilder;
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

    public QueryEngine(IAerospikeClient client, QueryContextBuilder queryContextBuilder,
                       FilterExpressionsBuilder filterExpressionsBuilder, AerospikeDataSettings dataSettings) {
        this.client = client;
        this.queryContextBuilder = queryContextBuilder;
        this.filterExpressionsBuilder = filterExpressionsBuilder;
        this.dataSettings = dataSettings;
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
     * @param namespace Namespace to store the data
     * @param set       Set storing the data
     * @param binNames  Bin names to return from the query
     * @param query     {@link Query} for filtering results
     * @return A KeyRecordIterator to iterate over the results
     */
    public KeyRecordIterator select(String namespace, String set, String[] binNames, @Nullable Query query) {
        // Query with filters
        if (isQueryCriteriaNotNull(query) && query.getCriteriaObject() != null) {
            // Provide dataSettings to use in FilterOperation
            query.getCriteriaObject().setDataSettings(dataSettings);
        }

        QueryContext queryContext = queryContextBuilder.build(namespace, set, query, binNames);
        Statement statement = queryContext.statement();
        statement.setMaxRecords(queryMaxRecords);
        QueryPolicy localQueryPolicy = getQueryPolicy(queryContext.qualifier(), true);

        if (!scansEnabled && statement.getFilter() == null) {
            throw new IllegalStateException(SCANS_DISABLED_MESSAGE);
        }

        RecordSet rs = client.query(localQueryPolicy, statement);
        try {
            return new KeyRecordIterator(namespace, rs);
        } catch (AerospikeException e) {
            if (statement.getFilter() != null && SEC_INDEX_ERROR_RESULT_CODES.contains(e.getResultCode())) {
                log.warn("Got secondary index related exception (resultCode: {}), " +
                    "retrying with filter expression only (scan operation)", e.getResultCode());
                Qualifier qualifier = isQueryCriteriaNotNull(query) ? query.getCriteriaObject() : null;
                return retryWithFilterExpression(namespace, qualifier, statement);
            }
            throw e;
        }
    }

    private KeyRecordIterator retryWithFilterExpression(String namespace, Qualifier qualifier, Statement statement) {
        // retry without sIndex filter
        QueryPolicy localQueryPolicyFallback = getQueryPolicy(qualifier, true);
        statement.setFilter(null);
        RecordSet rs = client.query(localQueryPolicyFallback, statement);
        return new KeyRecordIterator(namespace, rs);
    }

    /**
     * Select records filtered by a query to be counted
     *
     * @param namespace Namespace to store the data
     * @param set       Set storing the data
     * @param query     {@link Query} for filtering results
     * @return A KeyRecordIterator for counting
     */
    public KeyRecordIterator selectForCount(String namespace, String set, @Nullable Query query) {
        QueryContext queryContext = queryContextBuilder.build(namespace, set, query);
        Statement statement = queryContext.statement();
        statement.setMaxRecords(queryMaxRecords);
        Qualifier qualifier = queryContext.qualifier() != null ? queryContext.qualifier() : null;
        QueryPolicy localQueryPolicy = getQueryPolicy(qualifier, false);

        if (!scansEnabled && statement.getFilter() == null) {
            throw new IllegalStateException(SCANS_DISABLED_MESSAGE);
        }

        RecordSet rs = client.query(localQueryPolicy, statement);
        return new KeyRecordIterator(namespace, rs);
    }

    private QueryPolicy getQueryPolicy(Qualifier qualifier, boolean includeBins) {
        QueryPolicy queryPolicy = new QueryPolicy(client.getQueryPolicyDefault());
        queryPolicy.filterExp = filterExpressionsBuilder.build(qualifier);
        queryPolicy.includeBinData = includeBins;
        return queryPolicy;
    }
}
