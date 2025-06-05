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

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@Getter
public class Query {

    private final CriteriaDefinition criteria;
    private static final int NOT_SPECIFIED = -1;
    @Setter
    private long offset = NOT_SPECIFIED;
    @Setter
    private int rows = NOT_SPECIFIED;
    @Setter
    private Sort sort;
    @Setter
    private boolean isDistinct;

    /**
     * Creates new instance of {@link Query} with given criteria.
     *
     * @param criteria can be {@literal null}.
     */
    public Query(CriteriaDefinition criteria) {
        this.criteria = criteria;
    }

    /**
     * Get the {@link Qualifier} object.
     */
    public Qualifier getCriteriaObject() {
        return criteria.getCriteriaObject();
    }

    public boolean hasOffset() {
        return this.offset != NOT_SPECIFIED;
    }

    public boolean hasRows() {
        return this.rows != NOT_SPECIFIED;
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
                    "Given sort contained an Order for %s with ignoring case. "
                        + "Currently Aerospike does not support case-ignoring sorting",
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
