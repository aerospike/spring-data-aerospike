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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.util.StringUtils;

/**
 * @author peter
 * @author Anastasiia Smirnova
 */
public class StatementBuilder {

    private static final Logger log = LoggerFactory.getLogger(StatementBuilder.class);
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
        } else if (qualifiers != null && qualifiers.length != 0) {
            // statement's filter is set based on the first processed qualifier's filter
            // if the qualifier doesn't have EXCLUDE_FILTER set to true
            setStatementFilterFromQualifiers(stmt, qualifiers);
        }
        return stmt;
    }

    private void setStatementFilterFromQualifiers(Statement stmt, Qualifier[] qualifiers) {
        /*
         *  query with filters
         */
        for (Qualifier qualifier : qualifiers) {
            if (qualifier == null || qualifier.getExcludeFilter()) continue;
            if (qualifier.getOperation() == FilterOperation.AND) {
                // no sense to use secondary index in case of OR
                // as it requires to enlarge selection to more than 1 field
                for (Qualifier innerQualifier : qualifier.getQualifiers()) {
                    if (innerQualifier != null && isIndexedBin(stmt, innerQualifier)) {
                        Filter filter = innerQualifier.setQueryAsFilter();
                        if (filter != null) {
                            stmt.setFilter(filter);
                            innerQualifier.setQueryAsFilter(true);
                            break; // the first processed filter becomes statement filter
                        }
                    }
                }
            } else if (isIndexedBin(stmt, qualifier)) {
                Filter filter = qualifier.setQueryAsFilter();
                if (filter != null) {
                    stmt.setFilter(filter);
                    qualifier.setQueryAsFilter(true);
                    break; // the first processed filter becomes statement filter
                }
            }
        }
    }

    private boolean isIndexedBin(Statement stmt, Qualifier qualifier) {
        boolean hasIndex = false, hasField = false;
        if (StringUtils.hasLength(qualifier.getField())) {
            hasField = true;
            hasIndex = indexesCache.hasIndexFor(
                new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getField())
            );
        }

        if (log.isDebugEnabled() && hasField) {
            log.debug("Bin {}.{}.{} has secondary index: {}",
                stmt.getNamespace(), stmt.getSetName(), qualifier.getField(), hasIndex);
        }
        return hasIndex;
    }
}
