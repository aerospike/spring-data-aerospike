/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Aerospike specific data access operations.
 *
 * @author Oliver Gierke
 * @author Peter Milne
 * @author Anastasiia Smirnova
 * @author Roman Terentiev
 */
public interface AerospikeOperations {

    /**
     * Returns the set name used for the given entityClass in the namespace configured for the AerospikeTemplate in use.
     *
     * @param entityClass The class to get the set name for.
     * @return The set name used for the given entityClass.
     */
    <T> String getSetName(Class<T> entityClass);

    /**
     * @return mapping context in use.
     */
    MappingContext<?, ?> getMappingContext();

    /**
     * @return aerospike client in use.
     */
    IAerospikeClient getAerospikeClient();

    /**
     * Insert document using {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to insert. Must not be {@literal null}.
     */
    <T> void insert(T document);

    /**
     * Save document.
     * <p>
     * If document has version property - CAS algorithm is used for updating record.
     * Version property is used for deciding whether to create new record or update existing.
     * If version is set to zero - new record will be created, creation will fail is such record already exists.
     * If version is greater than zero - existing record will be updated with {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY} policy
     * taking into consideration the version property of the document.
     * Version property will be updated with the server's version after successful operation.
     * <p>
     * If document does not have version property - record is updated with {@link com.aerospike.client.policy.RecordExistsAction#REPLACE} policy.
     * This means that when such record does not exist it will be created, otherwise updated - an "upsert".
     *
     * @param document The document to save. Must not be {@literal null}.
     */
    <T> void save(T document);

    /**
     * Persist document using specified WritePolicy.
     *
     * @param document    The document to persist. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be {@literal null}.
     */
    <T> void persist(T document, WritePolicy writePolicy);

    /**
     * Insert each document of the given documents using single insert operations.
     *
     * @param documents The documents to insert. Must not be {@literal null}.
     */
    <T> void insertAll(Collection<? extends T> documents);

    /**
     * Update document using {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY} policy
     * taking into consideration the version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     */
    <T> void update(T document);

    /**
     * Update document specific fields based on a given collection of fields.
     * using {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy -
     * You can instantiate the document with only relevant fields and specify the list of fields that you want to update.
     * taking into consideration the version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     */
    <T> void update(T document, Collection<String> fields);

    /**
     * Truncate/Delete all the documents in the given entity's set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     */
    <T> void delete(Class<T> entityClass);

    /**
     * Delete document by id, set name will be determined by the given entityClass.
     *
     * @param id          The id of the document to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return whether the document existed on server before deletion.
     */
    <T> boolean delete(Object id, Class<T> entityClass);

    /**
     * Delete document.
     *
     * @param document The document to delete. Must not be {@literal null}.
     * @return whether the document existed on server before deletion.
     */
    <T> boolean delete(T document);

    /**
     * Check if document exists by providing document id and entityClass (set name will be determined by the given entityClass).
     *
     * @param id          The id to check if exists. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return whether the document exists.
     */
    <T> boolean exists(Object id, Class<T> entityClass);

    /**
     * Find documents in the given entityClass's set using a query and map them to the given class type.
     *
     * @param query       The query to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Stream<T> find(Query query, Class<T> entityClass);

    /**
     * Find documents in the given entityClass's set using a query and map them to the given target class type.
     *
     * @param query       The query to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Stream<S> find(Query query, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find all documents in the given entityClass's set and map them to the given class type.
     *
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Stream<T> findAll(Class<T> entityClass);

    /**
     * Find all documents in the given entityClass's set and map them to the given target class type.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Stream<S> findAll(Class<T> entityClass, Class<S> targetClass);

    /**
     * Find a document by id, set name will be determined by the given entityClass.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the document to. Must not be {@literal null}.
     * @return The document from Aerospike, returned document will be mapped to entityClass's type, if document doesn't exist return null.
     */
    <T> T findById(Object id, Class<T> entityClass);

    /**
     * Find a document by id, set name will be determined by the given entityClass.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return The document from Aerospike, returned document will be mapped to targetClass's type, if document doesn't exist return null.
     */
    <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find documents by providing multiple ids using a single batch read operation, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return The documents from Aerospike, returned documents will be mapped to entityClass's type, if no document exists return an empty list.
     */
    <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Find documents by providing multiple ids using a single batch read operation, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document exists return an empty list.
     */
    <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass);

    /**
     * Executes a single batch request to get results for several entities.
     * <p>
     * Aerospike provides functionality to get documents from different sets in 1 batch
     * request. The methods allow to put grouped keys by entity type as parameter and
     * get result as spring data aerospike entities grouped by entity type.
     *
     * @param groupedKeys Must not be {@literal null}.
     * @return grouped entities.
     */
    GroupedEntities findByIds(GroupedKeys groupedKeys);

    /**
     * Add integer/double bin values to existing document bin values, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param values   a Map of bin names and values to add. Must not be {@literal null}.
     * @return Modified document after add operations.
     */
    <T> T add(T document, Map<String, Long> values);

    /**
     * Add integer/double bin value to existing document bin value, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return Modified document after add operation.
     */
    <T> T add(T document, String binName, long value);

    /**
     * Append bin string values to existing document bin values, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param values   a Map of bin names and values to append. Must not be {@literal null}.
     * @return Modified document after append operations.
     */
    <T> T append(T document, Map<String, String> values);

    /**
     * Append bin string value to existing document bin value, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return Modified document after append operation.
     */
    <T> T append(T document, String binName, String value);

    /**
     * Prepend bin string values to existing document bin values, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param values   a Map of bin names and values to prepend. Must not be {@literal null}.
     * @return Modified document after prepend operations.
     */
    <T> T prepend(T document, Map<String, String> values);

    /**
     * Prepend bin string value to existing document bin value, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return Modified document after prepend operation.
     */
    <T> T prepend(T document, String binName, String value);

    /**
     * Execute query, apply statement's aggregation function, and return result iterator.
     *
     * @param filter      The filter to pass to the query.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param module      server package where user defined function resides.
     * @param function    aggregation function name.
     * @param arguments   arguments to pass to function name, if any.
     * @return Result iterator.
     */
    <T> Iterable<T> aggregate(Filter filter, Class<T> entityClass, String module, String function, List<Value> arguments);

    /**
     * Execute operation against underlying store.
     *
     * @param supplier must not be {@literal null}.
     * @return Execution result.
     */
    <T> T execute(Supplier<T> supplier);

    /**
     * Find all documents in the given entityClass's set using a provided sort and map them to the given class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to.
     * @return An Iterable of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Iterable<T> findAll(Sort sort, Class<T> entityClass);

    /**
     * Find all documents in the given entityClass's set using a provided sort and map them to the given target class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param entityClass The class to extract the Aerospike set from.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return An Iterable of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Iterable<S> findAll(Sort sort, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find documents in the given entityClass's set using a range (offset, limit) and a sort
     * and map them to the given class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the order of the returned Stream of documents.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Stream<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

    /**
     * Find documents in the given entityClass's set using a range (offset, limit) and a sort
     * and map them to the given target class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the returned Stream of documents order.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Stream<S> findInRange(long offset, long limit, Sort sort, Class<T> entityClass, Class<S> targetClass);

    /**
     * Return the amount of documents in a query results. set name will be determined by the given entityClass.
     *
     * @param query       The query that provides the result set for count.
     * @param entityClass entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return amount of documents that the given query and entity class supplied.
     */
    <T> long count(Query query, Class<T> entityClass);

    /**
     * Return the amount of documents in the given Aerospike set.
     *
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return amount of documents in the given set.
     */
    long count(String setName);

    /**
     * Return the amount of documents in the given entityClass's Aerospike set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return amount of documents in the set (of the given entityClass).
     */
    <T> long count(Class<T> entityClass);

    /**
     * Create index by specified name in Aerospike.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     * @param binName     The bin name to create the index on. Must not be {@literal null}.
     * @param indexType   The type of the index. Must not be {@literal null}.
     */
    <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                         IndexType indexType);

    /**
     * Create index by specified name in Aerospike.
     *
     * @param entityClass         The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     */
    <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                         IndexType indexType, IndexCollectionType indexCollectionType);

    /**
     * Create index by specified name in Aerospike.
     *
     * @param entityClass         The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     * @param ctx                 optional context to index on elements within a CDT.
     */
    <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                         IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx);

    /**
     * Delete index by specified name from Aerospike.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     */
    <T> void deleteIndex(Class<T> entityClass, String indexName);

    /**
     * Checks whether index by specified name exists in Aerospike.
     *
     * @param indexName The Aerospike index name. Must not be {@literal null}.
     * @return true if exists
     * @deprecated This operation is deprecated due to complications that are required for guaranteed index existence response.
     * <p>If you need to conditionally create index — replace this method (indexExists) with {@link #createIndex} and catch {@link IndexAlreadyExistsException}.
     * <p>More information can be found at: <a href="https://github.com/aerospike/aerospike-client-java/pull/149">https://github.com/aerospike/aerospike-client-java/pull/149</a>
     */
    @Deprecated
    boolean indexExists(String indexName);
}
