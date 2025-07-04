/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.aerospike.repository;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.qualifier.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.Repository;

/**
 * Aerospike specific {@link Repository}.
 *
 * @author Oliver Gierke
 * @author Peter Milne
 * @author Jean Mercier
 */
public interface AerospikeRepository<T, ID> extends PagingAndSortingRepository<T, ID>, CrudRepository<T, ID> {

    /**
     * Create an index with the specified name.
     *
     * @param domainType The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName  The index name. Must not be {@literal null}.
     * @param binName    The bin name to create the index on. Must not be {@literal null}.
     * @param indexType  The type of the index. Must not be {@literal null}.
     */
    <E> void createIndex(Class<E> domainType, String indexName, String binName, IndexType indexType);

    /**
     * Delete an index with the specified name.
     *
     * @param domainType The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName  The index name. Must not be {@literal null}.
     */
    <E> void deleteIndex(Class<E> domainType, String indexName);

    /**
     * Checks whether an index with the specified name exists in Aerospike.
     *
     * @param indexName The Aerospike index name.
     * @return true if exists.
     */
    boolean indexExists(String indexName);

    /**
     * Run a query to find entities.
     * <p>
     * A {@link Query} can be created using a qualifier. A {@link Qualifier} may contain other qualifiers and combine
     * them using either {@link FilterOperation#AND} or {@link FilterOperation#OR}.
     *
     * @param query A query to be performed. Must not be {@literal null}.
     * @return Iterable of entities.
     */
    Iterable<T> findUsingQuery(Query query);

    /**
     * Run a query to find entities providing a class to map entities to.
     * <p>
     * A {@link Query} can be created using a qualifier. A {@link Qualifier} may contain other qualifiers and combine
     * them using either {@link FilterOperation#AND} or {@link FilterOperation#OR}.
     *
     * @param query A query to be performed. Must not be {@literal null}.
     * @param targetClass Target class to map entities to.
     * @return Iterable of entities of the given target class.
     */
    <S> Iterable<S> findUsingQuery(Query query, Class<S> targetClass);
}
