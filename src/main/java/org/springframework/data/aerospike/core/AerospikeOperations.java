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
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;

import java.time.Instant;
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
     * Return set name used for the given entityClass in the namespace configured for the AerospikeTemplate in use.
     *
     * @param entityClass The class to get the set name for.
     * @return The set name used for the given entityClass.
     */
    <T> String getSetName(Class<T> entityClass);

    /**
     * Return set name used for the given document in the namespace configured for the AerospikeTemplate in use.
     *
     * @param document The document to get the set name for.
     * @return The set name used for the given document.
     */
    <T> String getSetName(T document);

    /**
     * @return Mapping context in use.
     */
    MappingContext<?, ?> getMappingContext();

    /**
     * @return converter in use.
     */
    MappingAerospikeConverter getAerospikeConverter();

    /**
     * @return server version support in use.
     */
    ServerVersionSupport getServerVersionSupport();

    /**
     * @return Aerospike client in use.
     */
    IAerospikeClient getAerospikeClient();

    /**
     * @return Value of configuration parameter {@link AerospikeDataSettings#getQueryMaxRecords()}.
     */
    long getQueryMaxRecords();

    /**
     * Save a document.
     * <p>
     * If the document has version property, CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create a new record or update an existing one. If the version is set to zero, a new record
     * will be created, creation will fail if such record already exists. If the version is greater than zero, existing
     * record will be updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined
     * with removing bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY})
     * taking into consideration the version property of the document. Document's version property will be updated with
     * the server's version after successful operation.
     * <p>
     * If the document does not have version property, record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated (an "upsert").
     *
     * @param document The document to be saved. Must not be {@literal null}.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> void save(T document);

    /**
     * Save a document within the given set (overrides the set associated with the document)
     * <p>
     * If the document has version property, CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create a new record or update an existing one. If the version is set to zero, a new record
     * will be created, creation will fail if such record already exists. If the version is greater than zero, existing
     * record will be updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined
     * with removing bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY})
     * taking into consideration the version property of the document. Document's version property will be updated with
     * the server's version after successful operation.
     * <p>
     * If the document does not have version property, record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated (an "upsert").
     *
     * @param document The document to be saved. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> void save(T document, String setName);

    /**
     * Save multiple documents in one batch request. The policies are analogous to {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents to be saved. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray If batch save succeeds, but results contain errors or null records.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void saveAll(Iterable<T> documents);

    /**
     * Save multiple documents within the given set (overrides the default set associated with the documents) in one
     * batch request. The policies are analogous to {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents to be saved. Must not be {@literal null}.
     * @param setName   Set name to override the default set associated with the documents.
     * @throws AerospikeException.BatchRecordArray If batch save succeeds, but results contain errors or null records.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void saveAll(Iterable<T> documents, String setName);

    /**
     * Insert a document using {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If the document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to be inserted. Must not be {@literal null}.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If batch operation failed (see
     *                                           {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void insert(T document);

    /**
     * Insert a document within the given set (overrides the set associated with the document) using
     * {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If document has version property, it will be updated with the server's version after successful operation.
     *
     * @param document The document to be inserted. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If batch operation failed (see
     *                                           {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void insert(T document, String setName);

    /**
     * Insert multiple documents in one batch request. The policies are analogous to {@link #insert(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to be inserted. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray If batch insert succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void insertAll(Iterable<? extends T> documents);

    /**
     * Insert multiple documents within the given set (overrides the set associated with the document) in one batch
     * request. The policies are analogous to {@link #insert(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents Documents to be inserted. Must not be {@literal null}.
     * @param setName   Set name to override the set associated with the document.
     * @throws AerospikeException.BatchRecordArray If batch insert succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void insertAll(Iterable<? extends T> documents, String setName);

    /**
     * Persist a document using specified WritePolicy.
     *
     * @param document    The document to be persisted. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void persist(T document, WritePolicy writePolicy);

    /**
     * Persist a document within the given set (overrides the default set associated with the document) using specified
     * WritePolicy.
     *
     * @param document    The document to be persisted. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     * @param setName     Set name to override the set associated with the document.
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void persist(T document, WritePolicy writePolicy, String setName);

    /**
     * Update a record using {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with
     * removing bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking
     * into consideration the version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> void update(T document);

    /**
     * Update a record with the given set (overrides the set associated with the document) using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> void update(T document, String setName);

    /**
     * Update specific fields of a record based on the given collection of fields using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy. You can instantiate the document with
     * only the relevant fields and specify the list of fields that you want to update. Taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @param fields   Specific fields to update.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> void update(T document, Collection<String> fields);

    /**
     * Update specific fields of a record based on the given collection of fields with the given set (overrides the set
     * associated with the document) using {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy.
     * You can instantiate the document with only the relevant fields and specify the list of fields that you want to
     * update. Taking into consideration the version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @param setName  Set name to override the set associated with the document.
     * @param fields   Specific fields to update.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> void update(T document, String setName, Collection<String> fields);

    /**
     * Update multiple records in one batch request. The policies are analogous to {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents that identify the records to be updated. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray If batch update succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void updateAll(Iterable<T> documents);

    /**
     * Update multiple records within the given set (overrides the default set associated with the documents) in one
     * batch request. The policies are analogous to {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents that identify the records to be updated. Must not be {@literal null}.
     * @param setName   Set name to override the set associated with the document.
     * @throws AerospikeException.BatchRecordArray If batch update succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void updateAll(Iterable<T> documents, String setName);

    /**
     * Truncate/Delete all records in the set determined by the given entityClass.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @deprecated since 4.6.0, use {@link AerospikeOperations#deleteAll(Class)} instead.
     */
    <T> void delete(Class<T> entityClass);

    /**
     * Delete a record by id, set name will be determined by the given entityClass.
     *
     * @param id          The id of the record to delete. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return whether the record existed on server before deletion.
     * @deprecated since 4.6.0, use {@link AerospikeOperations#deleteById(Object, Class)} instead.
     */
    <T> boolean delete(Object id, Class<T> entityClass);

    /**
     * Delete a record using the document's id.
     * <p>
     * If the document has version property it will be compared with the corresponding record's version on server.
     *
     * @param document The document to get set name and id from. Must not be {@literal null}.
     * @return Whether the record existed on server before deletion.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> boolean delete(T document);

    /**
     * Delete a record within the given set using the document's id.
     * <p>
     * If the document has version property it will be compared with the corresponding record's version on server.
     *
     * @param document The document to get id from. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @return Whether the record existed on server before deletion.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               If operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> boolean delete(T document, String setName);

    /**
     * Delete records using a query using the set associated with the given entityClass.
     *
     * @param query       The query to check if any matching records exist. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     */
    <T> void delete(Query query, Class<T> entityClass);

    /**
     * Delete records using a query within the given set.
     *
     * @param query       The query to check if any matching records exist. Must not be {@literal null}.
     * @param entityClass The class to translate to returned records into. Must not be {@literal null}.
     * @param setName     Set name to use. Must not be {@literal null}.
     */
    <T> void delete(Query query, Class<T> entityClass, String setName);

    /**
     * Count existing records by ids and a query using the given entityClass.
     * <p>
     * The records will be mapped to the given targetClass. The results are not processed (no pagination).
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    <T> void deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query);

    /**
     * Count existing records by ids and a query using the given entityClass within the set.
     * <p>
     * The records will be mapped to the given targetClass. The results are not processed (no pagination).
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param setName     Set name to use. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    <T> void deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, String setName, @Nullable Query query);

    /**
     * Delete multiple records in one batch request. The policies are analogous to {@link #delete(Object)}.
     * <p>
     * The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents to be deleted. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray If batch save succeeds, but results contain errors or null records.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void deleteAll(Iterable<T> documents);

    /**
     * Delete multiple records within the given set (overrides the default set associated with the documents) in one
     * batch request. The policies are analogous to {@link #delete(Object)}.
     * <p>
     * The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents to be deleted. Must not be {@literal null}.
     * @param setName   Set name to override the default set associated with the documents.
     * @throws AerospikeException.BatchRecordArray If batch delete results contain errors.
     * @throws OptimisticLockingFailureException   If at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void deleteAll(Iterable<T> documents, String setName);

    /**
     * Delete a record by id, set name will be determined by the given entityClass.
     * <p>
     * If the document has version property it is not compared with the corresponding record's version on server.
     *
     * @param id          The id of the record to be deleted. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return Whether the record existed on server before deletion.
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> boolean deleteById(Object id, Class<T> entityClass);

    /**
     * Delete a record by id within the given set.
     * <p>
     * If the document has version property it is not compared with the corresponding record's version on server.
     *
     * @param id      The id of the record to be deleted. Must not be {@literal null}.
     * @param setName Set name to use.
     * @return Whether the record existed on server before deletion.
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    boolean deleteById(Object id, String setName);

    /**
     * Delete records by ids using a single batch delete operation, set name will be determined by the given
     * entityClass. The policies are analogous to {@link #deleteById(Object, Class)}.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids         The ids of the records to be deleted. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray If batch delete results contain errors.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void deleteByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Delete records by ids within the given set using a single batch delete operation. The policies are analogous to
     * {@link #deleteById(Object, String)}.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids     The ids of the records to be deleted. Must not be {@literal null}.
     * @param setName Set name to use.
     * @throws AerospikeException.BatchRecordArray If batch delete results contain errors.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    void deleteByIds(Iterable<?> ids, String setName);

    /**
     * Perform a single batch delete operation for records from different sets.
     * <p>
     * Records' versions on server are not checked.
     * <p>
     * This operation requires Server 6.0+.
     *
     * @param groupedKeys Keys grouped by document type. Must not be {@literal null}, groupedKeys.getEntitiesKeys() must
     *                    not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray If batch delete results contain errors.
     * @throws DataAccessException                 If batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    void deleteByIds(GroupedKeys groupedKeys);

    /**
     * Truncate/Delete all records in the set determined by the given entity class.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void deleteAll(Class<T> entityClass);

    /**
     * Truncate/Delete all documents in the given set.
     *
     * @param entityClass      The class to extract set name from. Must not be {@literal null}.
     * @param beforeLastUpdate Delete records before the specified time (must be earlier than the current time at
     *                         millisecond resolution).
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> void deleteAll(Class<T> entityClass, Instant beforeLastUpdate);

    /**
     * Truncate/Delete all documents in the given set.
     *
     * @param setName Set name to truncate/delete all records in.
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    void deleteAll(String setName);

    /**
     * Truncate/Delete all documents in the given set.
     *
     * @param setName          Set name to truncate/delete all records in.
     * @param beforeLastUpdate Delete records before the specified time (must be earlier than the current time at
     *                         millisecond resolution).
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    void deleteAll(String setName, Instant beforeLastUpdate);

    /**
     * Find an existing record matching the document's class and id, add map values to the corresponding bins of the
     * record and return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the record to. Must not be {@literal null}.
     * @param values   The Map of bin names and values to add. Must not be {@literal null}.
     * @return Modified record mapped to the document's class.
     */
    <T> T add(T document, Map<String, Long> values);

    /**
     * Find an existing record matching the document's id and the given set name, add map values to the corresponding
     * bins of the record and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the record to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param values   The Map of bin names and values to add. Must not be {@literal null}.
     * @return Modified record mapped to the document's class.
     */
    <T> T add(T document, String setName, Map<String, Long> values);

    /**
     * Find an existing record matching the document's class and id, add specified value to the record's bin and return
     * the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the record to. Must not be {@literal null}.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return Modified record mapped to the document's class.
     */
    <T> T add(T document, String binName, long value);

    /**
     * Find an existing record matching the document's id and the given set name, add specified value to the record's
     * bin and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the record to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return Modified record mapped to the document's class.
     */
    <T> T add(T document, String setName, String binName, long value);

    /**
     * Find an existing record matching the document's class and id, append map values to the corresponding bins of the
     * record and return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the record to. Must not be {@literal null}.
     * @param values   The Map of bin names and values to append. Must not be {@literal null}.
     * @return Modified record mapped to the document's class.
     */
    <T> T append(T document, Map<String, String> values);

    /**
     * Find an existing record matching the document's id and the given set name, append map values to the corresponding
     * bins of the record and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the record to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param values   The Map of bin names and values to append. Must not be {@literal null}.
     * @return Modified record mapped to the document's class.
     */
    <T> T append(T document, String setName, Map<String, String> values);

    /**
     * Find an existing record matching the document's class and id, append specified value to the record's bin and
     * return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the record to. Must not be {@literal null}.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return Modified record mapped to the document's class.
     */
    <T> T append(T document, String binName, String value);

    /**
     * Find an existing record matching the document's id and the given set name, append specified value to the record's
     * bin and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the record to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return Modified record mapped to the document's class.
     */
    <T> T append(T document, String setName, String binName, String value);

    /**
     * Find an existing record matching the document's class and id, prepend map values to the corresponding bins of the
     * record and return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the record to. Must not be {@literal null}.
     * @param values   The Map of bin names and values to prepend. Must not be {@literal null}.
     * @return Modified record mapped to the document's class.
     */
    <T> T prepend(T document, Map<String, String> values);

    /**
     * Find an existing record matching the document's id and the given set name, prepend map values to the
     * corresponding bins of the record and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the record to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param values   The Map of bin names and values to prepend. Must not be {@literal null}.
     * @return Modified record mapped to the document's class.
     */
    <T> T prepend(T document, String setName, Map<String, String> values);

    /**
     * Find an existing record matching the document's class and id, prepend specified value to the record's bin and
     * return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the record to. Must not be {@literal null}.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return Modified record mapped to the document's class.
     */
    <T> T prepend(T document, String binName, String value);

    /**
     * Find an existing record matching the document's id and the given set name, prepend specified value to the
     * record's bin and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the record to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return Modified record mapped to the document's class.
     */
    <T> T prepend(T document, String setName, String binName, String value);

    /**
     * Execute an operation against underlying store.
     *
     * @param supplier must not be {@literal null}.
     * @return The resulting document.
     */
    <T> T execute(Supplier<T> supplier);

    /**
     * Find a record by id, set name will be determined by the given entityClass.
     * <p>
     * The matching record will be mapped to the given entityClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from and to map the record to. Must not be {@literal null}.
     * @return The document mapped to entityClass's type or null if nothing is found
     */
    <T> T findById(Object id, Class<T> entityClass);

    /**
     * Find a record by id within the given set.
     * <p>
     * The matching record will be mapped to the given entityClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to map the record to and to get entity properties from (such as expiration). Must
     *                    not be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return The record mapped to entityClass's type or null if document does not exist
     */
    <T> T findById(Object id, Class<T> entityClass, String setName);

    /**
     * Find a record by id, set name will be determined by the given entityClass.
     * <p>
     * The matching record will be mapped to the given entityClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return The record mapped to targetClass's type or null if document doesn't exist.
     */
    <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find a record by id within the given set.
     * <p>
     * The matching record will be mapped to the given entityClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to get entity properties from (such as expiration). Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return The record mapped to targetClass's type or null if document doesn't exist.
     */
    <T, S> S findById(Object id, Class<T> entityClass, Class<S> targetClass, String setName);

    /**
     * Find records by ids using a single batch read operation, set name will be determined by the given entityClass.
     * <p>
     * The records will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from and to map the records to. Must not be {@literal null}.
     * @return The matching records mapped to entityClass's type, if no document exists, an empty list is returned.
     */
    <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Find records by ids within the given set using a single batch read operation.
     * <p>
     * The records will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to map the records to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return The matching records mapped to entityClass's type or an empty list if nothing found.
     */
    <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass, String setName);

    /**
     * Find records by ids using a single batch read operation, set name will be determined by the given entityClass.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return The matching records mapped to targetClass's type or an empty list if nothing found.
     */
    <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find records by ids within the given set using a single batch read operation.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to get entity properties from (such as expiration). Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return The matching records mapped to targetClass's type or an empty list if nothing found.
     */
    <T, S> List<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass, String setName);

    /**
     * Execute a single batch request to find several records, possibly from different sets.
     * <p>
     * Aerospike provides functionality to get records from different sets in 1 batch request. This method receives keys
     * grouped by document type as a parameter and returns Aerospike records mapped to documents grouped by type.
     *
     * @param groupedKeys Keys grouped by document type. Must not be {@literal} null, groupedKeys.getEntitiesKeys() must
     *                    not be {@literal null}.
     * @return grouped documents.
     */
    GroupedEntities findByIds(GroupedKeys groupedKeys);

    /**
     * Find a record by id using a query, set name will be determined by the given entityClass.
     * <p>
     * The record will be mapped to the given targetClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching record mapped to targetClass's type.
     */
    <T, S> Object findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass, @Nullable Query query);

    /**
     * Find a record by id within the given set using a query.
     * <p>
     * The record will be mapped to the given targetClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to get the entity properties from (such as expiration). Must not be
     *                    {@literal null}.
     * @param targetClass The class to map the record to.
     * @param setName     Set name to use.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching record mapped to targetClass's type.
     */
    <T, S> Object findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass, String setName,
                                     @Nullable Query query);

    /**
     * Find records by ids and a query, set name will be determined by the given entityClass.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    <T, S> List<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                       @Nullable Query query);

    /**
     * Find records by ids within the given set.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to get the entity properties from (such as expiration). Must not be
     *                    {@literal null}.
     * @param targetClass The class to map the record to.
     * @param setName     Set name to use.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    <T, S> List<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass, String setName,
                                       @Nullable Query query);

    /**
     * Find records in the given entityClass's set using a query and map them to the given class type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract set name from and to map the records to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to entityClass type.
     */
    <T> Stream<T> find(Query query, Class<T> entityClass);

    /**
     * Find records in the given entityClass's set using a query and map them to the given target class type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to targetClass type.
     */
    <T, S> Stream<S> find(Query query, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find records in the given set using a query and map them to the given target class type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to targetClass type.
     */
    <T> Stream<T> find(Query query, Class<T> targetClass, String setName);

    /**
     * Find all records in the given entityClass's set and map them to the given class type.
     *
     * @param entityClass The class to extract set name from and to map the records to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to entityClass type.
     */
    <T> Stream<T> findAll(Class<T> entityClass);

    /**
     * Find all records in the given entityClass's set and map them to the given target class type.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to targetClass type.
     */
    <T, S> Stream<S> findAll(Class<T> entityClass, Class<S> targetClass);

    /**
     * Find all records in the given set and map them to the given class type.
     *
     * @param targetClass The class to map the records to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return A Stream of matching records mapped to entityClass type.
     */
    <T> Stream<T> findAll(Class<T> targetClass, String setName);

    /**
     * Find all records in the given entityClass's set using a provided sort and map them to the given class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract set name from and to map the records to.
     * @return A Stream of matching records mapped to entityClass type.
     */
    <T> Stream<T> findAll(Sort sort, long offset, long limit, Class<T> entityClass);

    /**
     * Find all records in the given entityClass's set using a provided sort and map them to the given target class
     * type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract set name from.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to targetClass type.
     */
    <T, S> Stream<S> findAll(Sort sort, long offset, long limit, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find all records in the given set using a provided sort and map them to the given target class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return A Stream of matching records mapped to targetClass type.
     */
    <T> Stream<T> findAll(Sort sort, long offset, long limit, Class<T> targetClass, String setName);

    /**
     * Find records in the given entityClass's set using a range (offset, limit) and a sort and map them to the given
     * class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the order of the returned Stream of documents.
     * @param entityClass The class to extract set name from and to map the records to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to entityClass type.
     */
    <T> Stream<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

    /**
     * Find records in the given entityClass's set using a range (offset, limit) and a sort and map them to the given
     * target class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the returned Stream of documents order.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Stream of matching records mapped to targetClass type.
     */
    <T, S> Stream<S> findInRange(long offset, long limit, Sort sort, Class<T> entityClass, Class<S> targetClass);

    /**
     * Find records in the given set using a range (offset, limit) and a sort and map them to the given target class
     * type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the returned Stream of documents order.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return A Stream of matching records mapped to targetClass type.
     */
    <T> Stream<T> findInRange(long offset, long limit, Sort sort, Class<T> targetClass, String setName);

    /**
     * Find records in the given entityClass set using a query and map them to the given target class type. If the query
     * has pagination and/or sorting, post-processing must be applied separately.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the records to.
     * @param query       The {@link Query} to filter results.
     * @return A Stream of all matching records (regardless of pagination/sorting) mapped to targetClass type.
     */
    <T, S> Stream<S> findUsingQueryWithoutPostProcessing(Class<T> entityClass, Class<S> targetClass, Query query);

    /**
     * Check by id if a record exists within the set associated with the given entityClass.
     *
     * @param id          The id to check for record existence. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return whether the matching record exists.
     */
    <T> boolean exists(Object id, Class<T> entityClass);

    /**
     * Check by id if a record exists within the given set name.
     *
     * @param id      The id to check for record existence. Must not be {@literal null}.
     * @param setName Set name to use.
     * @return whether the matching record exists.
     */
    boolean exists(Object id, String setName);

    /**
     * Check using a query if any matching records exist within the set associated with the given entityClass.
     *
     * @param query       The query to check if any matching records exist. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return whether any matching records exist.
     */
    <T> boolean exists(Query query, Class<T> entityClass);

    /**
     * Check using a query if any matching records exist within the given set.
     *
     * @param query   The query to check if any matching records exist. Must not be {@literal null}.
     * @param setName Set name to use. Must not be {@literal null}.
     * @return whether any matching records exist.
     */
    boolean exists(Query query, String setName);

    /**
     * Find if there are existing records by ids and a query using the given entityClass.
     * <p>
     * The records will not be mapped to the given entityClass. The results are not processed (no pagination).
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    <T> boolean existsByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query);

    /**
     * Find if there are existing records by ids and a query using the given entityClass within the set.
     * <p>
     * The records will not be mapped to a Java class. The results are not processed (no pagination).
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to use. Must not be {@literal null}.
     * @param query   The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    boolean existsByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query);

    /**
     * Return the amount of records in the set determined by the given entityClass.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return amount of records in the set of the given entityClass.
     */
    <T> long count(Class<T> entityClass);

    /**
     * Return the amount of records in the given Aerospike set.
     *
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return amount of records in the given set.
     */
    long count(String setName);

    /**
     * Return the amount of records in query results. Set name will be determined by the given entityClass.
     *
     * @param query       The query that provides the result set for count.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return amount of records matching the given query and entity class.
     */
    <T> long count(Query query, Class<T> entityClass);

    /**
     * Return the amount of records in query results within the given set.
     *
     * @param query   The query that provides the result set for count.
     * @param setName Set name to use.
     * @return amount of documents matching the given query and set.
     */
    long count(Query query, String setName);

    /**
     * Count existing records by ids and a query using the given entityClass.
     * <p>
     * The records will not be mapped to the given entityClass. The results are not processed (no pagination).
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    <T> long countByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query);

    /**
     * Count existing records by ids and a query using the given entityClass within the set.
     * <p>
     * The records will not be mapped to a Java class. The results are not processed (no pagination).
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to use. Must not be {@literal null}.
     * @param query   The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching records mapped to targetClass's type if provided (otherwise to entityClass's type), or an
     * empty list if no documents found.
     */
    long countByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query);

    /**
     * Execute query, apply statement's aggregation function, and return result iterator.
     *
     * @param filter      The filter to pass to the query.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param module      server package where user defined function resides.
     * @param function    aggregation function name.
     * @param arguments   arguments to pass to function name, if any.
     * @return Result iterator.
     */
    <T> ResultSet aggregate(Filter filter, Class<T> entityClass, String module, String function, List<Value> arguments);

    /**
     * Execute query within the given set, apply statement's aggregation function, and return result iterator.
     *
     * @param filter    The filter to pass to the query.
     * @param setName   Set name to use.
     * @param module    server package where user defined function resides.
     * @param function  aggregation function name.
     * @param arguments arguments to pass to function name, if any.
     * @return Result iterator.
     */
    ResultSet aggregate(Filter filter, String setName, String module, String function, List<Value> arguments);

    /**
     * Create an index with the specified name in Aerospike.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     * @param binName     The bin name to create the index on. Must not be {@literal null}.
     * @param indexType   The type of the index. Must not be {@literal null}.
     */
    <T> void createIndex(Class<T> entityClass, String indexName, String binName,
                         IndexType indexType);

    /**
     * Create an index with the specified name in Aerospike.
     *
     * @param entityClass         The class to extract set name from. Must not be {@literal null}.
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
     * @param entityClass         The class to extract set name from. Must not be {@literal null}.
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
     * @param setName   Set name to use.
     * @param indexName The index name. Must not be {@literal null}.
     * @param binName   The bin name to create the index on. Must not be {@literal null}.
     * @param indexType The type of the index. Must not be {@literal null}.
     */
    void createIndex(String setName, String indexName, String binName,
                     IndexType indexType);

    /**
     * Create an index with the specified name in Aerospike.
     *
     * @param setName             Set name to use.
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
     * @param setName             Set name to use.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     * @param ctx                 optional context to index on elements within a CDT.
     */
    void createIndex(String setName, String indexName, String binName,
                     IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx);

    /**
     * Delete an index with the specified name in Aerospike.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     */
    <T> void deleteIndex(Class<T> entityClass, String indexName);

    /**
     * Delete an index with the specified name within the given set in Aerospike.
     *
     * @param setName   Set name to use.
     * @param indexName The index name. Must not be {@literal null}.
     */
    void deleteIndex(String setName, String indexName);

    /**
     * Check whether an index with the specified name exists in Aerospike.
     *
     * @param indexName The Aerospike index name. Must not be {@literal null}.
     * @return true if exists
     */
    boolean indexExists(String indexName);
}
