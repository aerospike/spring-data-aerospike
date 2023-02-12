/*
 * Copyright 2015-2019 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.repository.query;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.keyvalue.core.query.KeyValueQuery;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
public class Query {

    private static final int NOT_SPECIFIED = -1;

    private Sort sort;
    private long offset = NOT_SPECIFIED;
    private int rows = NOT_SPECIFIED;
    private CriteriaDefinition criteria;

    /**
     * Creates new instance of {@link KeyValueQuery} with given criteria.
     *
     * @param criteria can be {@literal null}.
     */
    public Query(CriteriaDefinition criteria) {
        this.criteria = criteria;
    }

    /**
     * Creates new instance of {@link Query} with given {@link Sort}.
     *
     * @param sort can be {@literal null}.
     */
    public Query(Sort sort) {
        this.sort = sort;
    }

    /**
     * Get the criteria object.
     */
    public CriteriaDefinition getCriteria() {
        return criteria;
    }

    /**
     * Get {@link Sort}.
     */
    public Sort getSort() {
        return sort;
    }

    /**
     * Set {@link Sort} to be applied.
     */
    public void setSort(Sort sort) {
        this.sort = sort;
    }

    /**
     * Number of elements to skip.
     *
     * @return negative value if not set.
     */
    public long getOffset() {
        return this.offset;
    }

    /**
     * Set the number of elements to skip.
     *
     * @param offset use negative value for none.
     */
    public void setOffset(long offset) {
        this.offset = offset;
    }

    public boolean hasOffset() {
        return this.offset != NOT_SPECIFIED;
    }

    public boolean hasRows() {
        return this.rows != NOT_SPECIFIED;
    }

    /**
     * Number of elements to read.
     *
     * @return negative value if not set.
     */
    public int getRows() {
        return this.rows;
    }

    /**
     * Set the number of elements to read.
     */
    public void setRows(int rows) {
        this.rows = rows;
    }

    /**
     * Add given {@link Sort}.
     *
     * @param sort {@literal null} {@link Sort} will be ignored.
     */
    public Query orderBy(Sort sort) {
        if (sort == null) {
            return this;
        }

        if (this.sort != null) {
            this.sort.and(sort);
        } else {
            this.sort = sort;
        }
        return this;
    }

    /**
     * @see Query#setOffset(long)
     */
    public Query skip(long offset) {
        setOffset(offset);
        return this;
    }

    /**
     * @see Query#setRows(int)
     */
    public Query limit(int rows) {
        setRows(rows);
        return this;
    }

    public Query with(Sort sort) {
        if (sort == null) {
            return this;
        }

        for (Order order : sort) {
            if (order.isIgnoreCase()) {
                throw new IllegalArgumentException(String.format(
                    "Given sort contained an Order for %s with ignore case! "
                        + "Aerospike does not support sorting ignoring case currently!",
                    order.getProperty()));
            }
        }

        if (this.sort == null) {
            this.sort = sort;
        } else {
            this.sort = this.sort.and(sort);
        }

        return this;
    }
}
