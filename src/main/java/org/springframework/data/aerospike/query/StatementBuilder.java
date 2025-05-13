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
import static org.springframework.data.aerospike.query.QualifierUtils.queryCriteriaIsNotNull;
import static org.springframework.data.aerospike.util.Utils.logQualifierDetails;

/**
 * @author peter
 * @author Anastasiia Smirnova
 */
@Slf4j
public class StatementBuilder {

    private final IndexesCache indexesCache;

    public StatementBuilder(IndexesCache indexesCache) {
        this.indexesCache = indexesCache;
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
        Qualifier parentQualifier = query != null ? query.getCriteriaObject() : null;
        if (queryCriteriaIsNotNull(query)) {
            // logging query
            logQualifierDetails(query.getCriteriaObject(), log);
            // statement's filter is set based either on cardinality (the lowest bin values ratio)
            // or on order (the first processed filter)
            parentQualifier = setStatementFilterFromQualifiers(stmt, query.getCriteriaObject());
        }
        return new QueryContext(stmt, parentQualifier);
    }

    private Qualifier setStatementFilterFromQualifiers(Statement stmt, Qualifier parentQualifier) {
        // No qualifier, no need to set statement filter
        if (parentQualifier == null) return null;

        if (parentQualifier.getOperation() == FilterOperation.AND) {
            // Multiple qualifiers concatenated using logical AND
            // No sense to use secondary index in case of OR which requires to enlarge selection to more than 1 field
            return setFilterFromQualifiersCombinedWithAND(stmt, parentQualifier);
        } else if (isIndexedBin(stmt, parentQualifier)) {
            // Single qualifier
            return setFilterFromSingleQualifier(stmt, parentQualifier);
        }
        if (stmt.getFilter() != null) {
            log.debug("Query #{}, secondary index filter is set on the bin '{}'", parentQualifier.hashCode(),
                stmt.getFilter().getName());
        } else {
            log.debug("Query #{}, secondary index filter is not set", parentQualifier.hashCode());
        }
        // Return the initial parent qualifier as is
        return parentQualifier;
    }

    private Qualifier setFilterFromQualifiersCombinedWithAND(Statement stmt, Qualifier parentQualifier) {
        Qualifier minBinValuesRatioQualifier = getMinBinValuesRatioQualifier(parentQualifier, stmt);

        List<Qualifier> newInnerQualifiers;
        Qualifier newParentQualifier;
        // If index with min bin values ratio found, set filter with the matching qualifier
        if (minBinValuesRatioQualifier != null) {
            // Set secondary index Filter, return null qualifier if Filter is set
            setFilterFromSingleQualifier(stmt, minBinValuesRatioQualifier);

            if (stmt.getFilter() != null) {
                // Secondary index Filter was set, exclude the corresponding inner qualifier
                newInnerQualifiers =
                    getUpdatedInnerQualifiersWithCardinality(parentQualifier, minBinValuesRatioQualifier);
                newParentQualifier = getNewParentQualifierWithAND(parentQualifier, newInnerQualifiers);
            } else {
                // Filter wasn't set, continue as is
                newParentQualifier = parentQualifier;
            }
        } else {
            // No index with min bin values ratio found, do not consider cardinality when setting a Filter
            newInnerQualifiers = getUpdatedInnerQualifiersWithoutCardinality(parentQualifier, stmt);
            newParentQualifier = getNewParentQualifierWithAND(parentQualifier, newInnerQualifiers);
        }
        return newParentQualifier;
    }

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

    private Qualifier getNewParentQualifierWithAND(Qualifier parentQualifier, List<Qualifier> newInnerQualifiers) {
        Qualifier newParentQualifier = Qualifier.and(newInnerQualifiers.toArray(Qualifier[]::new));
        newParentQualifier.setDataSettings(parentQualifier.getDataSettings());
        return newParentQualifier;
    }

    /**
     * FilterExp is built only for qualifiers without secondary index filter or for dual filter operations that require
     * both secondary index filter and filter expression
     */
    private List<Qualifier> getUpdatedInnerQualifiersWithoutCardinality(Qualifier parentQualifier, Statement stmt) {
        List<Qualifier> newInnerQualifiers = new ArrayList<>();
        for (Qualifier innerQualifier : parentQualifier.getQualifiers()) {
            if (innerQualifier != null && isIndexedBin(stmt, innerQualifier)) {
                Filter filter = innerQualifier.getSecondaryIndexFilter();
                if (filter != null) {
                    // The filter from the first processed qualifier becomes statement's secondary index filter
                    stmt.setFilter(filter);
                    // Skip this inner qualifier in subsequent Exp building as it already has secondary index Filter
                    if (dualFilterOperations.contains(innerQualifier.getOperation())) {
                        // Still use the inner qualifier in case if it is a dual filter operation
                        newInnerQualifiers.add(innerQualifier);
                    }
                    break;
                } else {
                    newInnerQualifiers.add(innerQualifier);
                }
            } else {
                newInnerQualifiers.add(innerQualifier);
            }
        }
        return newInnerQualifiers;
    }

    private List<Qualifier> getUpdatedInnerQualifiersWithCardinality(Qualifier parentQualifier,
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

    /**
     * Returns null when secondary index filter is set, otherwise returns the given qualifier
     */
    private Qualifier setFilterFromSingleQualifier(Statement stmt, Qualifier qualifier) {
        Filter filter = qualifier.getSecondaryIndexFilter();
        if (filter != null) {
            stmt.setFilter(filter);
            return null;
        }
        return qualifier;
    }

    private boolean isIndexedBin(Statement stmt, Qualifier qualifier) {
        boolean hasIndexesForField = false, hasField = false;
        if (StringUtils.hasLength(qualifier.getBinName())) {
            hasField = true;
            hasIndexesForField = indexesCache.hasIndexFor(
                new IndexedField(stmt.getNamespace(), stmt.getSetName(), qualifier.getBinName())
            );
        }

        if (log.isDebugEnabled() && hasField) {
            log.debug("Qualifier #{}, bin {}.{}.{} has secondary index(es): {}", qualifier.hashCode(),
                stmt.getNamespace(), stmt.getSetName(), qualifier.getBinName(), hasIndexesForField);
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
