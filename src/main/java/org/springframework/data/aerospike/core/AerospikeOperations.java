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

import com.aerospike.client.AerospikeException;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.ResultSet;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;

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
     * Returns the set name used for the given entityClass in the namespace configured for the AerospikeTemplate in
     * use.
     *
     * @param entityClass The class to get the set name for.
     * @return The set name used for the given entityClass.
     */
    <T> String getSetName(Class<T> entityClass);

    /**
     * Returns the set name used for the given document in the namespace configured for the AerospikeTemplate in use.
     *
     * @param document The document to get the set name for.
     * @return The set name used for the given document.
     */
    <T> String getSetName(T document);

    /**
     * @return mapping context in use.
     */
    MappingContext<?, ?> getMappingContext();

    /**
     * @return aerospike client in use.
     */
    IAerospikeClient getAerospikeClient();

    /**
     * Save a document.
     * <p>
     * If the document has version property - CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create a new record or update an existing one. If the version is set to zero - new record
     * will be created, creation will fail is such record already exists. If version is greater than zero - existing
     * record will be updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined
     * with removing bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY})
     * taking into consideration the version property of the document. Version property will be updated with the
     * server's version after successful operation.
     * <p>
     * If the document does not have version property - record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated - an "upsert".
     *
     * @param document The document to save. Must not be {@literal null}.
     */
    <T> void save(T document);

    /**
     * Save a document with a given set name (overrides the set associated with the document)
     * <p>
     * If the document has version property - CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create a new record or update an existing one. If the version is set to zero - new record
     * will be created, creation will fail is such record already exists. If version is greater than zero - existing
     * record will be updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined
     * with removing bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY})
     * taking into consideration the version property of the document. Version property will be updated with the
     * server's version after successful operation.
     * <p>
     * If the document does not have version property - record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated - an "upsert".
     *
     * @param document The document to save. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     */
    <T> void save(T document, String setName);

    /**
     * Save multiple documents in one batch request. The policies are analogous to {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to save. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray         if batch save succeeds, but results contain errors or null
     *                                                     records
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    <T> void saveAll(Iterable<T> documents);

    /**
     * Save multiple documents in one batch request with a given set (overrides the set associated with the documents).
     * The policies are analogous to {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to save. Must not be {@literal null}.
     * @param setName   Set name to override the set associated with the documents.
     * @throws AerospikeException.BatchRecordArray         if batch save succeeds, but results contain errors or null
     *                                                     records
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    <T> void saveAll(Iterable<T> documents, String setName);

    /**
     * Insert a document using {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to insert. Must not be {@literal null}.
     */
    <T> void insert(T document);

    /**
     * Insert a document with a given set name (overrides the set associated with the document) using
     * {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to insert. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     */
    <T> void insert(T document, String setName);

    /**
     * Insert multiple documents in one batch request. The policies are analogous to {@link #insert(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to insert. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray         if batch insert succeeds, but results contain errors or null
     *                                                     records
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    <T> void insertAll(Iterable<? extends T> documents);

    /**
     * Insert multiple documents with a given set name (overrides the set associated with the document) in one batch
     * request. The policies are analogous to {@link #insert(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to insert. Must not be {@literal null}.
     * @param setName   Set name to override the set associated with the document.
     * @throws AerospikeException.BatchRecordArray         if batch insert succeeds, but results contain errors or null
     *                                                     records
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    <T> void insertAll(Iterable<? extends T> documents, String setName);

    /**
     * Persist a document using specified WritePolicy.
     *
     * @param document    The document to persist. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     */
    <T> void persist(T document, WritePolicy writePolicy);

    /**
     * Persist a document using specified WritePolicy and a given set (overrides the set associated with the document).
     *
     * @param document    The document to persist. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     * @param setName     Set name to override the set associated with the document.
     */
    <T> void persist(T document, WritePolicy writePolicy, String setName);

    /**
     * Update a document using {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with
     * removing bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking
     * into consideration the version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     */
    <T> void update(T document);

    /**
     * Update a document with a given set (overrides the set associated with the document) using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     */
    <T> void update(T document, String setName);

    /**
     * Update document's specific fields based on a given collection of fields using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy. You can instantiate the document with
     * only the relevant fields and specify the list of fields that you want to update. Taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @param fields   Specific fields to update.
     */
    <T> void update(T document, Collection<String> fields);

    /**
     * Update document's specific fields based on a given collection of fields with a given set (overrides the set
     * associated with the document). using {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy.
     * You can instantiate the document with only the relevant fields and specify the list of fields that you want to
     * update. Taking into consideration the version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     * @param fields   Specific fields to update.
     */
    <T> void update(T document, String setName, Collection<String> fields);

    /**
     * Update multiple documents in one batch request. The policies are analogous to {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to update. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray         if batch update succeeds, but results contain errors or null
     *                                                     records
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    <T> void updateAll(Iterable<T> documents);

    /**
     * Update multiple documents in one batch request with a given set (overrides the set associated with the
     * documents). The policies are analogous to {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to update. Must not be {@literal null}.
     * @param setName   Set name to override the set associated with the document.
     * @throws AerospikeException.BatchRecordArray         if batch update succeeds, but results contain errors or null
     *                                                     records
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    <T> void updateAll(Iterable<T> documents, String setName);

    /**
     * Truncate/Delete all the documents in the given entity's set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @deprecated since 4.6.0, use deleteAll(Class<T> entityClass) instead.
     */
    <T> void delete(Class<T> entityClass);

    /**
     * Delete a document by id, set name will be determined by the given entityClass.
     *
     * @param id          The id of the document to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return whether the document existed on server before deletion.
     * @deprecated since 4.6.0, use deleteById(Object id, Class<T> entityClass) instead.
     */
    <T> boolean delete(Object id, Class<T> entityClass);

    /**
     * Delete a document.
     *
     * @param document The document to delete. Must not be {@literal null}.
     * @return whether the document existed on server before deletion.
     */
    <T> boolean delete(T document);

    /**
     * Delete a document with a given set name.
     *
     * @param document The document to delete. Must not be {@literal null}.
     * @param setName  Set name to delete the document from.
     * @return whether the document existed on server before deletion.
     */
    <T> boolean delete(T document, String setName);

    /**
     * Delete a document by id, set name will be determined by the given entityClass.
     *
     * @param id          The id of the document to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return whether the document existed on server before deletion.
     */
    <T> boolean deleteById(Object id, Class<T> entityClass);

    /**
     * Delete a document by id with a given set name.
     *
     * @param id      The id of the document to delete. Must not be {@literal null}.
     * @param setName Set name to delete the document from.
     * @return whether the document existed on server before deletion.
     */
    boolean deleteById(Object id, String setName);

    /**
     * Delete documents by providing multiple ids using a single batch delete operation, set name will be determined by
     * the given entityClass.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids         The ids of the documents to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @throws AerospikeException.BatchRecordArray         if batch delete results contain errors
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    <T> void deleteByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Delete documents by providing multiple ids using a single batch delete operation with a given set.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids     The ids of the documents to delete. Must not be {@literal null}.
     * @param setName Set name to delete the documents from.
     * @throws AerospikeException.BatchRecordArray         if batch delete results contain errors
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    void deleteByIds(Iterable<?> ids, String setName);

    /**
     * Batch delete documents by providing their ids. Set name will be determined by the given entityClass.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids         The ids of the documents to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors or null records
     */
    <T> void deleteByIds(Collection<?> ids, Class<T> entityClass);

    /**
     * Batch delete documents by providing their ids with a given set name.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids     The ids of the documents to delete. Must not be {@literal null}.
     * @param setName Set name to delete the documents from.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors or null records
     */
    void deleteByIds(Collection<?> ids, String setName);

    /**
     * Executes a single batch delete for several entities.
     * <p>
     * Aerospike provides functionality to delete documents from different sets in 1 batch request. The methods allow to
     * put grouped keys by entity type as parameter and get result as spring data aerospike entities grouped by entity
     * type.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param groupedKeys Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray         if batch delete results contain errors
     * @throws org.springframework.dao.DataAccessException if batch operation failed (see
     *                                                     {@link DefaultAerospikeExceptionTranslator} for details)
     */
    void deleteByIds(GroupedKeys groupedKeys);

    /**
     * Truncate/Delete all the documents in the given entity's set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     */
    <T> void deleteAll(Class<T> entityClass);

    /**
     * Truncate/Delete all the documents in the given set.
     *
     * @param setName Set name to truncate/delete all the documents in.
     */
    void deleteAll(String setName);

    /**
     * Add integer/double bin values to existing document bin values, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param values   a Map of bin names and values to add. Must not be {@literal null}.
     * @return Modified document after add operations.
     */
    <T> T add(T document, Map<String, Long> values);

    /**
     * Add integer/double bin values to existing document bin values with a given set name, read the new modified
     * document and map it back to the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param setName  Set name for the operation.
     * @param values   a Map of bin names and values to add. Must not be {@literal null}.
     * @return Modified document after add operations.
     */
    <T> T add(T document, String setName, Map<String, Long> values);

    /**
     * Add integer/double bin value to existing document bin value, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return Modified document after add operation.
     */
    <T> T add(T document, String binName, long value);

    /**
     * Add integer/double bin value to existing document bin value with a given set name, read the new modified document
     * and map it back to the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param setName  Set name for the operation.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return Modified document after add operation.
     */
    <T> T add(T document, String setName, String binName, long value);

    /**
     * Append bin string values to existing document bin values, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param values   a Map of bin names and values to append. Must not be {@literal null}.
     * @return Modified document after append operations.
     */
    <T> T append(T document, Map<String, String> values);

    /**
     * Append bin string values to existing document bin values with a given set name, read the new modified document
     * and map it back to the given document class type.
     *
     * @param document The document to map the document to. Must not be {@literal null}.
     * @param setName  Set name for the operation.
     * @param values   a Map of bin names and values to append. Must not be {@literal null}.
     * @return Modified document after append operations.
     */
    <T> T append(T document, String setName, Map<String, String> values);

    /**
     * Append bin string value to existing document bin value, read the new modified document and map it back the given
     * document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return Modified document after append operation.
     */
    <T> T append(T document, String binName, String value);

    /**
     * Append bin string value to existing document bin value with a given set name, read the new modified document and
     * map it back to the given document class type.
     *
     * @param document The document to map the document to. Must not be {@literal null}.
     * @param setName  Set name for the operation.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return Modified document after append operation.
     */
    <T> T append(T document, String setName, String binName, String value);

    /**
     * Prepend bin string values to existing document bin values, read the new modified document and map it back the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param values   a Map of bin names and values to prepend. Must not be {@literal null}.
     * @return Modified document after prepend operations.
     */
    <T> T prepend(T document, Map<String, String> values);

    /**
     * Prepend bin string values to existing document bin values with a given set name, read the new modified document
     * and map it back to the given document class type.
     *
     * @param document The document to map the document to. Must not be {@literal null}.
     * @param setName  Set name for the operation.
     * @param values   a Map of bin names and values to prepend. Must not be {@literal null}.
     * @return Modified document after prepend operations.
     */
    <T> T prepend(T document, String setName, Map<String, String> values);

    /**
     * Prepend bin string value to existing document bin value, read the new modified document and map it back the given
     * document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return Modified document after prepend operation.
     */
    <T> T prepend(T document, String binName, String value);

    /**
     * Prepend bin string value to existing document bin value with a given set name, read the new modified document and
     * map it back to the given document class type.
     *
     * @param document The document to map the document to. Must not be {@literal null}.
     * @param setName  Set name for the operation.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return Modified document after prepend operation.
     */
    <T> T prepend(T document, String setName, String binName, String value);

    /**
     * Execute operation against underlying store.
     *
     * @param supplier must not be {@literal null}.
     * @return Execution result.
     */
    <T> T execute(Supplier<T> supplier);

    /**
     * Find a document by id, set name will be determined by the given entityClass.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the document to. Must not be
     *                    {@literal null}.
     * @return The document from Aerospike, returned document will be mapped to entityClass's type, if document doesn't
     * exist return null.
     */
    <T> T findById(Object id, Class<T> entityClass);

    /**
     * Find a document by id with a given set name.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to map the document to and to get entity properties from (such expiration). Must not
     *                    be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return The document from Aerospike, returned document will be mapped to entityClass's type, if document doesn't
     * exist return null.
     */
    <T> T findById(Object id, Class<T> entityClass, String setName);

    /**
     * Find a document by id, set name will be determined by the given entityClass.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return The document from Aerospike, returned document will be mapped to targetClass's type, if document doesn't
     * exist return null.
     */
    <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find a document by id with a given set name.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to get entity properties from (such as expiration). Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return The document from Aerospike, returned document will be mapped to targetClass's type, if document doesn't
     * exist return null.
     */
    <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass, String setName);

    /**
     * Find documents by providing multiple ids using a single batch read operation, set name will be determined by the
     * given entityClass.
     * <p>
     * Documents will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return The documents from Aerospike, returned documents will be mapped to entityClass's type, if no document
     * exists, an empty list is returned.
     */
    <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Find documents with a given set name by providing multiple ids using a single batch read operation.
     * <p>
     * Documents will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return The documents from Aerospike, returned documents will be mapped to entityClass's type, if no document
     * exists, an empty list is returned.
     */
    <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass, String setName);

    /**
     * Find documents by providing multiple ids using a single batch read operation, set name will be determined by the
     * given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document
     * exists, an empty list is returned.
     */
    <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find documents with a given set name by providing multiple ids using a single batch read operation.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to get entity properties from (such as expiration). Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document
     * exists, an empty list is returned.
     */
    <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass, String setName);

    /**
     * Executes a single batch request to get results for several entities.
     * <p>
     * Aerospike provides functionality to get documents from different sets in 1 batch request. The methods allow to
     * put grouped keys by entity type as parameter and get result as spring data aerospike entities grouped by entity
     * type.
     *
     * @param groupedKeys Must not be {@literal null}.
     * @return grouped entities.
     */
    GroupedEntities findByIds(GroupedKeys groupedKeys);

    /**
     * Find document by providing id, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to.
     * @param query       {@link Query} provided to build a filter expression. Optional argument.
     * @return The document from Aerospike, returned document will be mapped to targetClass's type.
     */
    <T, S> Object findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass,
                                     Query query);

    /**
     * Find document by providing id with a given set name.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to get the entity properties from (such as expiration). Must not be
     *                    {@literal null}.
     * @param targetClass The class to map the document to.
     * @param setName     Set name to find the document from.
     * @param query       {@link Query} provided to build a filter expression. Optional argument.
     * @return The document from Aerospike, returned document will be mapped to targetClass's type.
     */
    <T, S> Object findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass, String setName,
                                     Query query);

    /**
     * Find documents by providing multiple ids, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to.
     * @param query       {@link Query} provided to build a filter expression. Optional argument.
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document
     * exists, an empty list is returned.
     */
    <T, S> List<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass, Query query);

    /**
     * Find documents by providing multiple ids with a given set name.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to get the entity properties from (such as expiration). Must not be
     *                    {@literal null}.
     * @param targetClass The class to map the document to.
     * @param setName     Set name to find the document from.
     * @param query       {@link Query} provided to build a filter expression. Optional argument.
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document
     * exists, an empty list is returned.
     */
    <T, S> List<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass, String setName,
                                       Query query);

    /**
     * Find documents in the given entityClass's set using a query and map them to the given class type.
     *
     * @param query       The query to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
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
     * Find documents in the given set using a query and map them to the given target class type.
     *
     * @param query       The query to filter results. Must not be {@literal null}.
     * @param setName     Set name to find the documents in.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T> Stream<T> find(Query query, Class<T> targetClass, String setName);

    /**
     * Find all documents in the given entityClass's set using provided {@link Query}.
     *
     * @param query       Query to build filter expression from. Constructed using a {@link Qualifier} that can contain
     *                    other qualifiers. Must not be {@literal null}. If filter param is null and qualifier has
     *                    {@link Qualifier#getExcludeFilter()} == false, secondary index filter is built based on the
     *                    first processed qualifier.
     * @param entityClass The class to extract the Aerospike set from and to map the entity to. Must not be
     *                    {@literal null}.
     * @param filter      Secondary index filter.
     * @return Stream of entities.
     */
    <T> Stream<T> find(Query query, Class<T> entityClass, @Nullable Filter filter);

    /**
     * Find all documents in the given entityClass's set using provided {@link Query}.
     *
     * @param query       Query to build filter expression from. Constructed using a {@link Qualifier} that can contain
     *                    other qualifiers. Must not be {@literal null}. If filter param is null and qualifier has
     *                    {@link Qualifier#getExcludeFilter()} == false, secondary index filter is built based on the
     *                    first processed qualifier.
     * @param entityClass The class to extract the Aerospike set from and to map the entity to. Must not be
     *                    {@literal null}.
     * @param targetClass The class to map the entity to. Must not be {@literal null}.
     * @param filter      Secondary index filter.
     * @return Stream of entities.
     */
    <T, S> Stream<?> find(Query query, Class<T> entityClass, Class<S> targetClass, @Nullable Filter filter);

    /**
     * Find all documents in the given set using provided {@link Query}.
     *
     * @param query       Query to build filter expression from. Constructed using a {@link Qualifier} that can contain
     *                    other qualifiers. Must not be {@literal null}. If filter param is null and qualifier has
     *                    {@link Qualifier#getExcludeFilter()} == false, secondary index filter is built based on the
     *                    first processed qualifier.
     * @param targetClass The class to map the entity to. Must not be {@literal null}.
     * @param setName     Set name to find the documents in.
     * @param filter      Secondary index filter.
     * @return Stream of entities.
     */
    <T> Stream<T> find(Query query, Class<T> targetClass, String setName, @Nullable Filter filter);

    /**
     * Find all documents in the given entityClass's set and map them to the given class type.
     *
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
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
     * Find all documents in the given set and map them to the given class type.
     *
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     Set name to find all documents.
     * @return A Stream of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Stream<T> findAll(Class<T> targetClass, String setName);

    /**
     * Find all documents in the given entityClass's set using a provided sort and map them to the given class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to.
     * @return A stream of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Stream<T> findAll(Sort sort, long offset, long limit, Class<T> entityClass);

    /**
     * Find all documents in the given entityClass's set using a provided sort and map them to the given target class
     * type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract the Aerospike set from.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A stream of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Stream<S> findAll(Sort sort, long offset, long limit, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find all documents in the given set using a provided sort and map them to the given target class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @param setName     Set name to find the documents.
     * @return A stream of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T> Stream<T> findAll(Sort sort, long offset, long limit, Class<T> targetClass, String setName);

    /**
     * Find documents in the given entityClass's set using a range (offset, limit) and a sort and map them to the given
     * class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the order of the returned Stream of documents.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return A Stream of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Stream<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

    /**
     * Find documents in the given entityClass's set using a range (offset, limit) and a sort and map them to the given
     * target class type.
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
     * Find documents in the given set using a range (offset, limit) and a sort and map them to the given target class
     * type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the returned Stream of documents order.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @param setName     Set name to find the documents.
     * @return A Stream of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T> Stream<T> findInRange(long offset, long limit, Sort sort, Class<T> targetClass, String setName);

    /**
     * Check if a document exists by providing document id and entityClass (set name will be determined by the given
     * entityClass).
     *
     * @param id          The id to check if exists. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return whether the document exists.
     */
    <T> boolean exists(Object id, Class<T> entityClass);

    /**
     * Check if a document exists by providing document id and a set name.
     *
     * @param id      The id to check if exists. Must not be {@literal null}.
     * @param setName Set name to check if document exists.
     * @return whether the document exists.
     */
    boolean exists(Object id, String setName);

    /**
     * Check if any document exists by defining a query and entityClass (set name will be determined by the given
     * entityClass).
     *
     * @param query       The query to check if any returned document exists. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return whether any document exists.
     */
    <T> boolean existsByQuery(Query query, Class<T> entityClass);

    /**
     * Check if any document exists by defining a query, entityClass and a given set name.
     *
     * @param query       The query to check if any returned document exists. Must not be {@literal null}.
     * @param entityClass The class to translate to returned results into. Must not be {@literal null}.
     * @param setName     The set name to check if documents exists in. Must not be {@literal null}.
     * @return whether any document exists.
     */
    <T> boolean existsByQuery(Query query, Class<T> entityClass, String setName);

    /**
     * Return the amount of documents in the given entityClass's Aerospike set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return amount of documents in the set (of the given entityClass).
     */
    <T> long count(Class<T> entityClass);

    /**
     * Return the amount of documents in the given Aerospike set.
     *
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return amount of documents in the given set.
     */
    long count(String setName);

    /**
     * Return the amount of documents in query results. Set name will be determined by the given entityClass.
     *
     * @param query       The query that provides the result set for count.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return amount of documents that the given query and entity class supplied.
     */
    <T> long count(Query query, Class<T> entityClass);

    /**
     * Return the amount of documents in query results with a given set name.
     *
     * @param query   The query that provides the result set for count.
     * @param setName Set name to count the documents from.
     * @return amount of documents that the given query and entity class supplied.
     */
    long count(Query query, String setName);

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
    <T> ResultSet aggregate(Filter filter, Class<T> entityClass, String module, String function, List<Value> arguments);

    /**
     * Execute query with a given set name, apply statement's aggregation function, and return result iterator.
     *
     * @param filter    The filter to pass to the query.
     * @param setName   Set name to aggregate the documents from.
     * @param module    server package where user defined function resides.
     * @param function  aggregation function name.
     * @param arguments arguments to pass to function name, if any.
     * @return Result iterator.
     */
    ResultSet aggregate(Filter filter, String setName, String module, String function, List<Value> arguments);

    /**
     * Create an index with the specified name in Aerospike.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     * @param binName     The bin name to create the index on. Must not be {@literal null}.
     * @param indexType   The type of the index. Must not be {@literal null}.
     */
    <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                         IndexType indexType);

    /**
     * Create an index with the specified name in Aerospike.
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
     * Create an index with the specified name in Aerospike.
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
     * Create an index with the specified name in Aerospike.
     *
     * @param setName   Set name to create the index.
     * @param indexName The index name. Must not be {@literal null}.
     * @param binName   The bin name to create the index on. Must not be {@literal null}.
     * @param indexType The type of the index. Must not be {@literal null}.
     */
    void createIndex(String setName, String indexName, String binName,
                     IndexType indexType);

    /**
     * Create an index with the specified name in Aerospike.
     *
     * @param setName             Set name to create the index.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     */
    void createIndex(String setName, String indexName, String binName,
                     IndexType indexType, IndexCollectionType indexCollectionType);

    /**
     * Create an index with the specified name in Aerospike.
     *
     * @param setName             Set name to create the index.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     * @param ctx                 optional context to index on elements within a CDT.
     */
    void createIndex(String setName, String indexName, String binName,
                     IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx);

    /**
     * Delete an index with the specified name from Aerospike.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     */
    <T> void deleteIndex(Class<T> entityClass, String indexName);

    /**
     * Delete an index with the specified name from Aerospike with a given set name.
     *
     * @param setName   Set name to delete the index from.
     * @param indexName The index name. Must not be {@literal null}.
     */
    void deleteIndex(String setName, String indexName);

    /**
     * Checks whether an index with the specified name exists in Aerospike.
     *
     * @param indexName The Aerospike index name. Must not be {@literal null}.
     * @return true if exists
     */
    boolean indexExists(String indexName);
}
