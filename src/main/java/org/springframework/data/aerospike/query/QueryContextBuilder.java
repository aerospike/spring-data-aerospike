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
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.model.Index;
import org.springframework.data.aerospike.query.model.IndexedField;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.query.FilterOperation.AND;
import static org.springframework.data.aerospike.query.FilterOperation.dualFilterOperations;
import static org.springframework.data.aerospike.query.QualifierUtils.isQueryCriteriaNotNull;
import static org.springframework.data.aerospike.util.Utils.logQualifierDetails;

@Slf4j
public class QueryContextBuilder {

    private final IndexesCache indexesCache;

    public QueryContextBuilder(IndexesCache indexesCache) {
        this.indexesCache = indexesCache;
    }

    // Stores a list of inner qualifiers for building filter Exp and a secondary index Filter
    private record QualifiersWithFilter(List<Qualifier> innerQualifiers, @Nullable Filter filter) {

    }

    public QueryContext build(String namespace, String set, Query query) {
        return build(namespace, set, query, null);
    }

    public QueryContext build(String namespace, String set, @Nullable Query query, String[] binNames) {
        Statement stmt = new Statement();
        stmt.setNamespace(namespace);
        stmt.setSetName(set);
        if (binNames != null && binNames.length != 0) {
            stmt.setBinNames(binNames);
        }

        Qualifier processedParentQualifier = null;
        if (isQueryCriteriaNotNull(query)) {
            // logging query
            logQualifierDetails(query.getCriteriaObject(), log);

            if (!query.getCriteriaObject().hasFilterExpression()) {
                // Process qualifier and apply filters
                // Statement's filter is set based either on cardinality (the lowest bin values ratio)
                // or on order (the first processed filter)
                processedParentQualifier = setFilterAndProcessQualifier(stmt, query.getCriteriaObject());
            } else {
                // statement's filter is set based on parsed DSL expression
                stmt.setFilter(query.getCriteriaObject().getFilter());
                processedParentQualifier = query.getCriteriaObject();
            }
        }
        return new QueryContext(stmt, processedParentQualifier);
    }

    /**
     * Applies secondary index filter to the statement and processes the parent qualifier (excludes a qualifier used for
     * creating secondary index filter).
     *
     * @return A potentially modified parent qualifier
     */
    private Qualifier setFilterAndProcessQualifier(Statement stmt, Qualifier parentQualifier) {
        // No qualifier, no need to set statement filter
        if (parentQualifier == null) return null;

        Qualifier resultQualifier;
        if (parentQualifier.getOperation() == FilterOperation.AND) {
            // Multiple qualifiers concatenated using logical AND
            // No sense to use secondary index in case of OR which requires to enlarge selection to more than 1 field
            resultQualifier = setFilterAndProcessCombinedQualifier(stmt, parentQualifier);
        } else if (parentQualifier.hasSecondaryIndexName() || isIndexedBin(stmt, parentQualifier)) {
            // Single qualifier
            resultQualifier = setFilterAndProcessSingleQualifier(stmt, parentQualifier);
        } else {
            resultQualifier = parentQualifier;
        }

        // Log filter status
        logFilterStatus(stmt, parentQualifier);
        return resultQualifier;
    }

    /**
     * Sets secondary index Filter and returns updated AND-combined parent qualifier with excluded inner qualifier that
     * the Filter is based on
     */
    private Qualifier setFilterAndProcessCombinedQualifier(Statement stmt, Qualifier parentQualifier) {
        Qualifier qualifierChosenByCardinality = getMinBinValuesRatioQualifier(parentQualifier, stmt);
        if (qualifierChosenByCardinality != null) {
            // A qualifier based on cardinality (with minimal bin values ratio) is found
            Filter filter = qualifierChosenByCardinality.getSecondaryIndexFilter();
            stmt.setFilter(filter);
            return processCombinedQualifierWithCardinality(parentQualifier, qualifierChosenByCardinality, filter);
        }
        // No qualifier based on cardinality found
        QualifiersWithFilter qualifiersWithFilter = processCombinedQualifierWithoutCardinality(parentQualifier, stmt);
        if (qualifiersWithFilter.filter() != null) {
            stmt.setFilter(qualifiersWithFilter.filter());
            return getNewParentQualifierForAND(parentQualifier, qualifiersWithFilter.innerQualifiers());
        }
        return parentQualifier;
    }

    /**
     * Logs information about whether a filter was applied
     */
    private static void logFilterStatus(Statement stmt, Qualifier qualifier) {
        if (stmt.getFilter() != null) {
            log.debug("Query #{}, secondary index filter is set on the bin '{}'",
                qualifier.hashCode(), stmt.getFilter().getName());
        } else {
            log.debug("Query #{}, secondary index filter is not set", qualifier.hashCode());
        }
    }

    /**
     * Gets secondary index filter of the given qualifier, then sets the filter and returns null if the filter exists,
     * otherwise returns the initial qualifier
     */
    private static Qualifier setFilterAndProcessSingleQualifier(Statement stmt, Qualifier qualifier) {
        Filter filter = qualifier.getSecondaryIndexFilter();
        if (filter != null) {
            stmt.setFilter(filter);
            return null;
        }
        return qualifier;
    }

    /**
     * Processes AND-combined qualifier by excluding cardinality-based inner qualifier with secondary index filter
     */
    private Qualifier processCombinedQualifierWithCardinality(Qualifier parentQualifier,
                                                              Qualifier qualifierChosenByCardinality, Filter filter) {
        if (filter != null) {
            // If secondary index filter is set, exclude the corresponding inner qualifier
            List<Qualifier> updatedQualifiers =
                getUpdatedInnerQualifiersWithCardinality(parentQualifier, qualifierChosenByCardinality);
            return getNewParentQualifierForAND(parentQualifier, updatedQualifiers);
        }
        // Filter wasn't set, continue as is
        return parentQualifier;
    }

    /**
     * Returns the first qualifier with the lowest bin values ratio
     */
    private Qualifier getMinBinValuesRatioQualifier(Qualifier parentQualifier, Statement stmt) {
        int minBinValuesRatio = Integer.MAX_VALUE;
        Qualifier minBinValuesRatioQualifier = null;
        for (Qualifier innerQualifier : parentQualifier.getQualifiers()) {
            if (innerQualifier != null && isIndexedBin(stmt, innerQualifier)) {
                int currBinValuesRatio = getMinBinValuesRatioForQualifier(stmt, innerQualifier);
                // Compare the cardinality of each qualifier and select the qualifier that has the index with
                // the lowest bin values ratio
                if (currBinValuesRatio != 0 && currBinValuesRatio < minBinValuesRatio) {
                    minBinValuesRatio = currBinValuesRatio;
                    minBinValuesRatioQualifier = innerQualifier;
                }
            }
        }
        return minBinValuesRatioQualifier;
    }

    /**
     * Returns a new AND-combined parent qualifier with the given inner qualifiers and with data settings of the
     * initial parent qualifier
     */
    private static Qualifier getNewParentQualifierForAND(Qualifier parentQualifier,
                                                         List<Qualifier> newInnerQualifiers) {
        Qualifier newParentQualifier = Qualifier.and(newInnerQualifiers.toArray(Qualifier[]::new));
        newParentQualifier.setDataSettings(parentQualifier.getDataSettings());
        return newParentQualifier;
    }

    /**
     * Returns Filter and updated inner qualifiers excluding the one with secondary index Filter which is not dual (dual
     * filter operations require both secondary index filter and filter expression) when there is no qualifier based on
     * cardinality
     */
    private QualifiersWithFilter processCombinedQualifierWithoutCardinality(Qualifier parentQualifier,
                                                                            Statement stmt) {
        List<Qualifier> newInnerQualifiers = new ArrayList<>();
        Filter filter = null;
        for (Qualifier innerQualifier : parentQualifier.getQualifiers()) {
            if (innerQualifier != null && isIndexedBin(stmt, innerQualifier)) {
                // Filter from the first processed qualifier
                filter = innerQualifier.getSecondaryIndexFilter();
                if (filter != null) {
                    // Skip this inner qualifier in subsequent Exp building as it already has secondary index Filter
                    if (dualFilterOperations.contains(innerQualifier.getOperation())) {
                        // Still use the inner qualifier in case if it is a dual filter operation
                        newInnerQualifiers.add(innerQualifier);
                    }
                    continue;
                }
            }
            newInnerQualifiers.add(innerQualifier);
        }
        return new QualifiersWithFilter(newInnerQualifiers, filter);
    }


    /**
     * Returns updated inner qualifiers list excluding inner qualifier with secondary index Filter which is not dual
     * (dual filter operations require both secondary index filter and filter expression)
     */
    private static List<Qualifier> getUpdatedInnerQualifiersWithCardinality(Qualifier parentQualifier,
                                                                            Qualifier minBinValuesRatioQualifier) {
        return Arrays.stream(parentQualifier.getQualifiers())
            .flatMap(innerQualifier -> {
                // Look for inner qualifier to exclude if there is AND combination
                if (innerQualifier.hasQualifiers() && innerQualifier.getOperation() == AND) {
                    List<Qualifier> innerQualifiersToAdd =
                        getUpdatedInnerQualifiersWithCardinality(innerQualifier, minBinValuesRatioQualifier);
                    return innerQualifiersToAdd.stream();
                }
                // Return inner qualifier(s) as is
                return Stream.of(innerQualifier);
            })
            .filter(innerQualifier -> {
                // If this inner qualifier is chosen for building secondary index Filter based on cardinality
                if (innerQualifier.equals(minBinValuesRatioQualifier)) {
                    // Exclude it unless it is required for dual filter operations
                    return dualFilterOperations.contains(innerQualifier.getOperation());
                }
                return true;
            })
            .collect(Collectors.toList());
    }

    private boolean isIndexedBin(Statement stmt, Qualifier qualifier) {
        boolean hasIndexesForField = false;
        if (StringUtils.hasLength(qualifier.getBinName())) {
            hasIndexesForField = indexesCache.hasIndexFor(
                new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getBinName())
            );

            if (log.isDebugEnabled()) {
                log.debug("Qualifier #{}, bin {}.{}.{} has secondary index(es): {}", qualifier.hashCode(),
                    stmt.getNamespace(), stmt.getSetName(), qualifier.getBinName(), hasIndexesForField);
            }
        }

        return hasIndexesForField;
    }

    private int getMinBinValuesRatioForQualifier(Statement stmt, Qualifier qualifier) {
        // Get all indexes for field
        List<Index> indexList = indexesCache.getAllIndexesForField(
            new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getBinName()));

        // Return the lowest bin values ratio of the indexes in indexList
        Optional<Index> minBinValuesRatio = indexList.stream()
            .filter(index -> index.getBinValuesRatio() != 0)
            .min(Comparator.comparing(Index::getBinValuesRatio));

        return minBinValuesRatio.map(Index::getBinValuesRatio).orElse(0);
    }
}
