/*
 * Copyright 2019 the original author or authors.
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
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Aerospike specific data access operations to work with reactive API
 *
 * @author Igor Ermolenko
 */
public interface ReactiveAerospikeOperations {

    /**
     * @return Mapping context in use.
     */
    MappingContext<?, ?> getMappingContext();

    /**
     * @return Converter in use.
     */
    MappingAerospikeConverter getAerospikeConverter();

    /**
     * @return Aerospike reactive client in use.
     */
    IAerospikeReactorClient getAerospikeReactorClient();

    /**
     * @return Value of configuration parameter {@link AerospikeDataSettings#getQueryMaxRecords()}.
     */
    long getQueryMaxRecords();

    /**
     * Reactively save a document.
     * <p>
     * If the document has version property, CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create new record or update existing. If the version is set to zero, a new record will be
     * created, creation will fail if such record already exists. If version is greater than zero, existing record will
     * be updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing
     * bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into
     * consideration the version property of the document. Version property will be updated with the server's version
     * after successful operation.
     * <p>
     * If the document does not have version property, the record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated (an "upsert").
     *
     * @param document The document to be saved. Must not be {@literal null}.
     * @return A Mono of the new saved document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> save(T document);

    /**
     * Reactively save a document within the given set.
     * <p>
     * If the document has version property, CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create new record or update existing. If the version is set to zero, a new record will be
     * created, creation will fail if such record already exists. If version is greater than zero, existing record will
     * be updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing
     * bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into
     * consideration the version property of the document. Version property will be updated with the server's version
     * after successful operation.
     * <p>
     * If the document does not have version property, the record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated (an "upsert").
     *
     * @param document The document to be saved. Must not be {@literal null}.
     * @param setName  The set name to save the document.
     * @return A Mono of the new saved document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> save(T document, String setName);

    /**
     * Reactively save documents using one batch request. The policies are analogous to {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents The documents to be saved. Must not be {@literal null}.
     * @return A Flux of the saved documents
     * @throws AerospikeException.BatchRecordArray if batch save succeeds, but results contain errors or null records.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Flux<T> saveAll(Iterable<T> documents);

    /**
     * Reactively save documents within the given set using one batch request. The policies are analogous to
     * {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents The documents to be saved. Must not be {@literal null}.
     * @param setName   The set name to save to documents.
     * @return A Flux of the saved documents
     * @throws AerospikeException.BatchRecordArray if batch save succeeds, but results contain errors or null records.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Flux<T> saveAll(Iterable<T> documents, String setName);

    /**
     * Reactively insert a document using {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If the document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to be inserted. Must not be {@literal null}.
     * @return A Mono of the new inserted document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> insert(T document);

    /**
     * Reactively insert a document within the given set using
     * {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If the document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to be inserted. Must not be {@literal null}.
     * @param setName  The set name to insert the document.
     * @return A Mono of the new inserted document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> insert(T document, String setName);

    /**
     * Reactively insert documents using one batch request. The policies are analogous to {@link #insert(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents Documents to insert. Must not be {@literal null}.
     * @return A Flux of the inserted documents
     * @throws AerospikeException.BatchRecordArray if batch insert succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Flux<T> insertAll(Iterable<T> documents);

    /**
     * Reactively insert documents within the given set using one batch request. The policies are analogous to
     * {@link #insert(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents Documents to insert. Must not be {@literal null}.
     * @param setName   The set name to insert the documents.
     * @return A Flux of the inserted documents
     * @throws AerospikeException.BatchRecordArray if batch insert succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Flux<T> insertAll(Iterable<T> documents, String setName);

    /**
     * Reactively persist a document using specified WritePolicy.
     *
     * @param document    The document to be persisted. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     * @return A Mono of the new persisted document.
     * @throws DataAccessException if operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<T> persist(T document, WritePolicy writePolicy);

    /**
     * Reactively persist a document within the given set (overrides the default set associated with the document) using
     * specified WritePolicy.
     *
     * @param document    The document to be persisted. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     * @param setName     Set name to override the set associated with the document.
     * @return A Mono of the new persisted document.
     * @throws DataAccessException if operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<T> persist(T document, WritePolicy writePolicy, String setName);

    /**
     * Reactively update a record using {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy
     * combined with removing bins at first (analogous to
     * {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into consideration the version
     * property of the document if it is present.
     * <p>
     * If the document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @return A Mono of the new updated document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> update(T document);

    /**
     * Reactively update a record within the given set using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @param setName  The set name to update the document.
     * @return A Mono of the new updated document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> update(T document, String setName);

    /**
     * Reactively update specific bins of a record based on the given collection of fields using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy. You can instantiate the document with
     * only relevant fields and specify the list of fields that you want to update, taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @return A Mono of the new updated document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> update(T document, Collection<String> fields);

    /**
     * Reactively update specific fields of a record based on the given collection of fields within the given set using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy. You can instantiate the document with
     * only relevant fields and specify the list of fields that you want to update, taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document that identifies the record to be updated. Must not be {@literal null}.
     * @param setName  The set name to update the document.
     * @return A Mono of the new updated document.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<T> update(T document, String setName, Collection<String> fields);

    /**
     * Reactively update records using one batch request. The policies are analogous to {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents The documents that identify the records to be updated. Must not be {@literal null}.
     * @return A Flux of the updated documents
     * @throws AerospikeException.BatchRecordArray if batch update succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Flux<T> updateAll(Iterable<T> documents);

    /**
     * Reactively update records within the given set using one batch request. The policies are analogous to
     * {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents The documents that identify the records to be updated. Must not be {@literal null}.
     * @param setName   The set name to update the documents.
     * @return A Flux of the updated documents
     * @throws AerospikeException.BatchRecordArray if batch update succeeds, but results contain errors or null
     *                                             records.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Flux<T> updateAll(Iterable<T> documents, String setName);

    /**
     * Reactively delete a record using the document's id.
     * <p>
     * If the document has version property it will be compared with the corresponding record's version on server.
     *
     * @param document The document to get set name and id from. Must not be {@literal null}.
     * @return A Mono of whether the record existed on server before deletion.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<Boolean> delete(T document);

    /**
     * Reactively delete a record within the given set using the document's id.
     *
     * @param document The document to get id from. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @return A Mono of whether the record existed on server before deletion.
     * @throws OptimisticLockingFailureException if the document has a version attribute with a different value from
     *                                           that found on server.
     * @throws DataAccessException               if operation failed (see {@link DefaultAerospikeExceptionTranslator}
     *                                           for details).
     */
    <T> Mono<Boolean> delete(T document, String setName);

    /**
     * Delete records using a query using the set associated with the given entityClass.
     *
     * @param query       The query to check if any matching records exist. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     */
    <T> Mono<Void> delete(Query query, Class<T> entityClass);

    /**
     * Delete records using a query within the given set.
     *
     * @param query       The query to check if any matching records exist. Must not be {@literal null}.
     * @param entityClass The class to translate to returned records into. Must not be {@literal null}.
     * @param setName     Set name to use. Must not be {@literal null}.
     */
    <T> Mono<Void> delete(Query query, Class<T> entityClass, String setName);

    /**
     * Count existing records by ids and a query using the given entityClass.
     * <p>
     * The records will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     */
    <T> Mono<Void> deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query);

    /**
     * Count existing records by ids and a query using the given entityClass within the set.
     * <p>
     * The records will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param setName     Set name to use. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     */
    <T> Mono<Void> deleteByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, String setName,
                                         @Nullable Query query);

    /**
     * Reactively delete multiple records in one batch request. The policies are analogous to {@link #delete(Object)}.
     * <p>
     * The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents to be deleted. Must not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray if batch save succeeds, but results contain errors or null records.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<Void> deleteAll(Iterable<? extends T> documents);

    /**
     * Reactively delete multiple records within the given set (overrides the default set associated with the documents)
     * in one batch request. The policies are analogous to {@link #delete(Object)}.
     * <p>
     * The execution order is NOT preserved.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param documents The documents to be deleted. Must not be {@literal null}.
     * @param setName   Set name to override the default set associated with the documents.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors.
     * @throws OptimisticLockingFailureException   if at least one document has a version attribute with a different
     *                                             value from that found on server.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<Void> deleteAll(Iterable<? extends T> documents, String setName);

    /**
     * Reactively delete a record by id, set name will be determined by the given entity class.
     * <p>
     * If the document has version property it is not compared with the corresponding record's version on server.
     *
     * @param id          The id of the record to be deleted. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set name from. Must not be {@literal null}.
     * @return A Mono of whether the record existed on server before deletion.
     * @throws DataAccessException if operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<Boolean> deleteById(Object id, Class<T> entityClass);

    /**
     * Reactively delete a record by id within the given set.
     * <p>
     * If the document has version property it is not compared with the corresponding record's version on server.
     *
     * @param id      The id of the record to be deleted. Must not be {@literal null}.
     * @param setName Set name to use.
     * @return A Mono of whether the record existed on server before deletion.
     * @throws DataAccessException if operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    Mono<Boolean> deleteById(Object id, String setName);

    /**
     * Reactively delete records using a single batch delete operation, set name will be determined by the given entity
     * class. The policies are analogous to {@link #deleteById(Object, Class)}.
     * <p>
     * Deleting non-existing records results in {@link AerospikeException.BatchRecordArray} exception.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids         The ids of the records to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set name from and to map the results to. Must not be
     *                    {@literal null}.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<Void> deleteByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Reactively delete records using a single batch delete operation, set name will be determined by the given entity
     * class. The policies are analogous to {@link #deleteById(Object, Class)}.
     * <p>
     * Non-existing records are ignored.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids             The ids of the records to find. Must not be {@literal null}.
     * @param entityClass     The class to extract the Aerospike set name from and to map the results to. Must not be
     *                        {@literal null}.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<Void> deleteExistingByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Reactively delete records within the given set using a single batch delete operation. The policies are analogous
     * to {@link #deleteById(Object, String)}.
     * <p>
     * Deleting non-existing records results in {@link AerospikeException.BatchRecordArray} exception.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to use.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    Mono<Void> deleteByIds(Iterable<?> ids, String setName);

    /**
     * Reactively delete records within the given set using a single batch delete operation. The policies are analogous
     * to {@link #deleteById(Object, String)}.
     * <p>
     * Non-existing records are ignored.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to use.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    Mono<Void> deleteExistingByIds(Iterable<?> ids, String setName);

    /**
     * Reactively delete records from different sets in a single request.
     * <p>
     * Records' versions on server are not checked.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param groupedKeys Keys grouped by document type. Must not be {@literal null}, groupedKeys.getEntitiesKeys() must
     *                    not be {@literal null}.
     * @throws AerospikeException.BatchRecordArray if batch delete results contain errors.
     * @throws DataAccessException                 if batch operation failed (see
     *                                             {@link DefaultAerospikeExceptionTranslator} for details).
     */
    Mono<Void> deleteByIds(GroupedKeys groupedKeys);

    /**
     * Reactively truncate/delete all records in the set determined by the given entity class.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @throws DataAccessException if operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<Void> deleteAll(Class<T> entityClass);

    /**
     * Reactively truncate/delete all records in the set determined by the given entity class.
     *
     * @param entityClass      The class to extract set name from. Must not be {@literal null}.
     * @param beforeLastUpdate Delete records before the specified time (must be earlier than the current time at
     *                         millisecond resolution).
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    <T> Mono<Void> deleteAll(Class<T> entityClass, Instant beforeLastUpdate);

    /**
     * Reactively truncate/delete all the documents in the given set.
     *
     * @param setName Set name to use.
     * @throws DataAccessException if operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    Mono<Void> deleteAll(String setName);

    /**
     * Reactively truncate/delete all documents in the given set.
     *
     * @param setName          Set name to truncate/delete all records in.
     * @param beforeLastUpdate Delete records before the specified time (must be earlier than the current time at
     *                         millisecond resolution).
     * @throws DataAccessException If operation failed (see {@link DefaultAerospikeExceptionTranslator} for details).
     */
    Mono<Void> deleteAll(String setName, Instant beforeLastUpdate);

    /**
     * Find an existing record matching the document's class and id, add map values to the corresponding bins of the
     * record and return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the result to. Must not be {@literal null}.
     * @param values   The Map of bin names and values to add. Must not be {@literal null}.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> add(T document, Map<String, Long> values);

    /**
     * Find an existing record matching the document's id and the given set name, add map values to the corresponding
     * bins of the record and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the result to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param values   The Map of bin names and values to add. Must not be {@literal null}.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> add(T document, String setName, Map<String, Long> values);

    /**
     * Find an existing record matching the document's class and id, add specified value to the record's bin and return
     * the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the result to. Must not be {@literal null}.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> add(T document, String binName, long value);

    /**
     * Find an existing record matching the document's id and the given set name, add specified value to the record's
     * bin and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the result to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> add(T document, String setName, String binName, long value);

    /**
     * Find an existing record matching the document's class and id, append map values to the corresponding bins of the
     * record and return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the result to. Must not be {@literal null}.
     * @param values   The Map of bin names and values to append. Must not be {@literal null}.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> append(T document, Map<String, String> values);

    /**
     * Find an existing record matching the document's id and the given set name, append map values to the corresponding
     * bins of the record and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the result to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param values   The Map of bin names and values to append. Must not be {@literal null}.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> append(T document, String setName, Map<String, String> values);

    /**
     * Find an existing record matching the document's class and id, append specified value to the record's bin and
     * return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the result to. Must not be {@literal null}.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> append(T document, String binName, String value);

    /**
     * Find an existing record matching the document's id and the given set name, append specified value to the record's
     * bin and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the result to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> append(T document, String setName, String binName, String value);

    /**
     * Find an existing record matching the document's class and id, prepend map values to the corresponding bins of the
     * record and return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the result to. Must not be {@literal null}.
     * @param values   The Map of bin names and values to prepend. Must not be {@literal null}.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> prepend(T document, Map<String, String> values);

    /**
     * Find an existing record matching the document's id and the given set name, prepend map values to the
     * corresponding bins of the record and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the result to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param values   The Map of bin names and values to prepend. Must not be {@literal null}.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> prepend(T document, String setName, Map<String, String> values);

    /**
     * Find an existing record matching the document's class and id, prepend specified value to the record's bin and
     * return the modified record mapped to the document's class.
     *
     * @param document The document to get set name and id from and to map the result to. Must not be {@literal null}.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> prepend(T document, String binName, String value);

    /**
     * Find an existing record matching the document's id and the given set name, prepend specified value to the
     * record's bin and return the modified record mapped to the document's class.
     *
     * @param document The document to get id from and to map the result to. Must not be {@literal null}.
     * @param setName  Set name to use.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return A Mono of the modified record mapped to the document's class.
     */
    <T> Mono<T> prepend(T document, String setName, String binName, String value);

    /**
     * Reactively execute operation against underlying store.
     *
     * @param supplier must not be {@literal null}.
     * @return A Mono of the execution result.
     */
    <T> Mono<T> execute(Supplier<T> supplier);

    /**
     * Reactively find a record by id, set name will be determined by the given entityClass.
     * <p>
     * The matching record will be mapped to the given entityClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from and to map the document to. Must not be {@literal null}.
     * @return A Mono of the matching record mapped to entityClass type.
     */
    <T> Mono<T> findById(Object id, Class<T> entityClass);

    /**
     * Reactively find a record by id within the given set.
     * <p>
     * The matching record will be mapped to the given entityClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to map the record to and to get entity properties from (such expiration). Must not
     *                    be {@literal null}.
     * @param setName     Set name to use.
     * @return A Mono of the matching record mapped to entityClass type.
     */
    <T> Mono<T> findById(Object id, Class<T> entityClass, String setName);

    /**
     * Reactively find a record by id, set name will be determined by the given entityClass.
     * <p>
     * The matching record will be mapped to the given targetClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Mono of the matching record mapped to targetClass type.
     */
    <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find a record by id within the given set.
     * <p>
     * The matching record will be mapped to the given targetClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to map the record to and to get entity properties from (such expiration). Must not
     *                    be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return A Mono of the matching record mapped to targetClass type.
     */
    <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass, String setName);

    /**
     * Reactively find records by ids using a single batch read operation, set name will be determined by the given
     * entityClass.
     * <p>
     * The records will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Reactively find records by ids using a single batch read operation, set name will be determined by the given
     * entityClass.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to targetClass type.
     */
    <T, S> Flux<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find records by ids within the given set using a single batch read operation.
     * <p>
     * The records will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> targetClass, String setName);

    /**
     * Reactively execute a single batch request to find several records, possibly from different sets.
     * <p>
     * Aerospike provides functionality to get records from different sets in 1 batch request. This method receives keys
     * grouped by document type as a parameter and returns Aerospike records mapped to documents grouped by type.
     *
     * @param groupedKeys Keys grouped by document type. Must not be {@literal} null, groupedKeys.getEntitiesKeys() must
     *                    not be {@literal null}.
     * @return Mono of grouped documents.
     */
    Mono<GroupedEntities> findByIds(GroupedKeys groupedKeys);

    /**
     * Find a record by id using a query, set name will be determined by the given entityClass.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching record mapped to targetClass's type.
     */
    <T, S> Mono<?> findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass,
                                      @Nullable Query query);

    /**
     * Find a record by id within the given set using a query.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param id          The id of the record to find. Must not be {@literal null}.
     * @param entityClass The class to get the entity properties from (such as expiration). Must not be
     *                    {@literal null}.
     * @param targetClass The class to map the record to.
     * @param setName     Set name to use.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The matching record mapped to targetClass's type.
     */
    <T, S> Mono<?> findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass, String setName,
                                      @Nullable Query query);

    /**
     * Find records by ids, set name will be determined by the given entityClass.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type if provided
     * (otherwise to entityClass's type), if no document exists, an empty list is returned.
     */
    <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                       @Nullable Query query);

    /**
     * Find records by ids within the given set name.
     * <p>
     * The records will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to get the entity properties from (such as expiration). Must not be
     *                    {@literal null}.
     * @param targetClass The class to map the record to.
     * @param setName     Set name to use.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type if provided
     * (otherwise to entityClass's type), if no document exists, an empty list is returned.
     */
    <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass, String setName,
                                       @Nullable Query query);

    /**
     * Reactively find records in the given entityClass's set using a query and map them to the given class type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract set name from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> find(Query query, Class<T> entityClass);

    /**
     * Reactively find records in the given entityClass's set using a query and map them to the given target class
     * type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to targetClass type.
     */
    <T, S> Flux<S> find(Query query, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find records in the given set using a query and map them to the given target class type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return A Flux of matching records mapped to targetClass type.
     */
    <T> Flux<T> find(Query query, Class<T> targetClass, String setName);

    /**
     * Reactively find all records in the given entityClass's set and map them to the given class type.
     *
     * @param entityClass The class to extract set name from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> findAll(Class<T> entityClass);

    /**
     * Reactively find all records in the given entityClass's set and map them to the given target class type.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to targetClass type.
     */
    <T, S> Flux<S> findAll(Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find all records in the given set and map them to the given class type.
     *
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     The set name to find the document.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> findAll(Class<T> targetClass, String setName);

    /**
     * Reactively find all records in the given entityClass's set using a provided sort and map them to the given class
     * type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract set name from and to map the documents to.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> findAll(Sort sort, long offset, long limit, Class<T> entityClass);

    /**
     * Reactively find all records in the given entityClass's set using a provided sort and map them to the given target
     * class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract set name from.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to targetClass type.
     */
    <T, S> Flux<S> findAll(Sort sort, long offset, long limit, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find all records in the given entityClass's set using a provided sort and map them to the given target
     * class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     The set name to find the documents.
     * @return A Flux of matching records mapped to targetClass type.
     */
    <T> Flux<T> findAll(Sort sort, long offset, long limit, Class<T> targetClass, String setName);

    /**
     * Reactively find records in the given entityClass's set using a range (offset, limit) and a sort and map them to
     * the given class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the order of the returned Stream of documents.
     * @param entityClass The class to extract set name from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

    /**
     * Reactively find records in the given set using a range (offset, limit) and a sort and map them to the given class
     * type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the order of the returned Stream of documents.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @param setName     Set name to use.
     * @return A Flux of matching records mapped to entityClass type.
     */
    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> targetClass, String setName);

    /**
     * Reactively find records in the given entityClass's set using a range (offset, limit) and a sort and map them to
     * the given target class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the returned Stream of documents order.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to. Must not be {@literal null}.
     * @return A Flux of matching records mapped to targetClass type.
     */
    <T, S> Flux<S> findInRange(long offset, long limit, Sort sort, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find records in the given entityClass's set using a query and map them to the given target class type.
     * If the query has pagination and/or sorting, post-processing must be applied separately.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param targetClass The class to map the record to.
     * @param query       The {@link Query} to filter results.
     * @return A Flux of all matching records mapped to targetClass type (regardless of pagination/sorting).
     */
    <T, S> Flux<S> findUsingQueryWithoutPostProcessing(Class<T> entityClass, Class<S> targetClass, Query query);

    /**
     * Reactively check by id if a record exists within the set associated with the given entityClass.
     *
     * @param id          The id to check for record existence. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return A Mono of whether the record exists.
     */
    <T> Mono<Boolean> exists(Object id, Class<T> entityClass);

    /**
     * Reactively check by id if a record exists within the given set name.
     *
     * @param id      The id to check for record existence. Must not be {@literal null}.
     * @param setName Set name to use.
     * @return A Mono of whether the record exists.
     */
    Mono<Boolean> exists(Object id, String setName);

    /**
     * Reactively check using a query if any matching records exist within the set determined by the given entityClass.
     *
     * @param query       The query to check if any returned document exists. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return A Mono of whether matching records exist.
     */
    <T> Mono<Boolean> exists(Query query, Class<T> entityClass);

    /**
     * Reactively check using a query if any matching records exist within the given set.
     *
     * @param query   The query to check if any matching records exist. Must not be {@literal null}.
     * @param setName Set name to use. Must not be {@literal null}.
     * @return A Mono of whether matching records exist.
     */
    Mono<Boolean> exists(Query query, String setName);

    /**
     * Find if there are existing records by ids and a query using the given entityClass.
     * <p>
     * The records will not be mapped to the given entityClass. The results are not processed (no pagination).
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return True if matching records exist, otherwise false.
     */
    <T> Mono<Boolean> existsByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query);

    /**
     * Find if there are existing records by ids and a query using the given entityClass within the set.
     * <p>
     * The records will not be mapped to a Java class. The results are not processed (no pagination).
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to use. Must not be {@literal null}.
     * @param query   The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return True if matching records exist, otherwise false.
     */
    Mono<Boolean> existsByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query);

    /**
     * Reactively return the amount of records in the set determined by the given entityClass.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @return A Mono of the amount of records in the set (of the given entityClass).
     */
    <T> Mono<Long> count(Class<T> entityClass);

    /**
     * Reactively return the amount of records in the given Aerospike set.
     *
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return A Mono of the amount of records in the given set.
     */
    Mono<Long> count(String setName);

    /**
     * Reactively return the amount of records in query results. Set name will be determined by the given entityClass.
     *
     * @param query       The query that provides the result set for count.
     * @param entityClass entityClass to extract set name from. Must not be {@literal null}.
     * @return A Mono of the amount of records matching the given query and entity class.
     */
    <T> Mono<Long> count(Query query, Class<T> entityClass);

    /**
     * Reactively return the amount of records in query results within the given set.
     *
     * @param query   The query that provides the result set for count.
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return A Mono of the amount of records matching the given query and set name.
     */
    Mono<Long> count(Query query, String setName);

    /**
     * Count existing records by ids and a query using the given entityClass.
     * <p>
     * The records will not be mapped to the given entityClass. The results are not processed (no pagination).
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return Quantity of matching queries.
     */
    <T> Mono<Long> countExistingByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, @Nullable Query query);

    /**
     * Count existing records by ids and a query using the given entityClass within the set.
     * <p>
     * The records will not be mapped to a Java class. The results are not processed (no pagination).
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to use. Must not be {@literal null}.
     * @param query   The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return Quantity of matching queries.
     */
    Mono<Long> countExistingByIdsUsingQuery(Collection<?> ids, String setName, @Nullable Query query);

    /**
     * Reactively create an index with the specified name in Aerospike.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     * @param binName     The bin name to create the index on. Must not be {@literal null}.
     * @param indexType   The type of the index. Must not be {@literal null}.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                               String binName, IndexType indexType);

    /**
     * Reactively create an index with the specified name in Aerospike.
     *
     * @param entityClass         The class to extract set name from. Must not be {@literal null}.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName, String binName,
                               IndexType indexType, IndexCollectionType indexCollectionType);

    /**
     * Reactively create an index with the specified name in Aerospike.
     *
     * @param entityClass         The class to extract set name from. Must not be {@literal null}.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     * @param ctx                 optional context to index on elements within a CDT.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName, String binName,
                               IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx);

    /**
     * Reactively create an index with the specified name in Aerospike.
     *
     * @param setName   Set name to use.
     * @param indexName The index name. Must not be {@literal null}.
     * @param binName   The bin name to create the index on. Must not be {@literal null}.
     * @param indexType The type of the index. Must not be {@literal null}.
     */
    Mono<Void> createIndex(String setName, String indexName,
                           String binName, IndexType indexType);

    /**
     * Reactively create an index with the specified name in Aerospike.
     *
     * @param setName             Set name to use.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     */
    Mono<Void> createIndex(String setName, String indexName, String binName,
                           IndexType indexType, IndexCollectionType indexCollectionType);

    /**
     * Reactively create an index with the specified name in Aerospike.
     *
     * @param setName             Set name to use.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     * @param ctx                 Optional context to index elements within a CDT.
     */
    Mono<Void> createIndex(String setName, String indexName, String binName,
                           IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx);

    /**
     * Reactively delete an index with the specified name in Aerospike.
     *
     * @param entityClass The class to extract set name from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     */
    <T> Mono<Void> deleteIndex(Class<T> entityClass, String indexName);

    /**
     * Reactively delete an index with the specified name within the given set in Aerospike.
     *
     * @param setName   Set name to use.
     * @param indexName The index name. Must not be {@literal null}.
     */
    Mono<Void> deleteIndex(String setName, String indexName);

    /**
     * Check whether an index with the specified name exists in Aerospike.
     *
     * @param indexName The Aerospike index name. Must not be {@literal null}.
     * @return Mono of true if exists.
     */
    Mono<Boolean> indexExists(String indexName);
}
