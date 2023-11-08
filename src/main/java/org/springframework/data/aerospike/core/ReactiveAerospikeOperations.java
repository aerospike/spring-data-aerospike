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
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.annotation.Nullable;

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
     * @return mapping context in use.
     */
    MappingContext<?, ?> getMappingContext();

    /**
     * @return aerospike reactive client in use.
     */
    IAerospikeReactorClient getAerospikeReactorClient();

    /**
     * Reactively save document.
     * <p>
     * If document has version property - CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create new record or update existing. If version is set to zero - new record will be created,
     * creation will fail is such record already exists. If version is greater than zero - existing record will be
     * updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing
     * bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into
     * consideration the version property of the document. Version property will be updated with the server's version
     * after successful operation.
     * <p>
     * If document does not have version property - record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated - an "upsert".
     *
     * @param document The document to save. Must not be {@literal null}.
     * @return A Mono of the new saved document.
     */
    <T> Mono<T> save(T document);

    /**
     * Reactively save document with a given set name.
     * <p>
     * If document has version property - CAS algorithm is used for updating record. Version property is used for
     * deciding whether to create new record or update existing. If version is set to zero - new record will be created,
     * creation will fail is such record already exists. If version is greater than zero - existing record will be
     * updated with {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing
     * bins at first (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into
     * consideration the version property of the document. Version property will be updated with the server's version
     * after successful operation.
     * <p>
     * If document does not have version property - record is updated with
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE}). This means that when such record
     * does not exist it will be created, otherwise updated - an "upsert".
     *
     * @param document The document to save. Must not be {@literal null}.
     * @param setName  The set name to save the document.
     * @return A Mono of the new saved document.
     */
    <T> Mono<T> save(T document, String setName);

    /**
     * Reactively save documents using one batch request. The policies are analogous to {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents documents to save. Must not be {@literal null}.
     * @return A Flux of the saved documents, otherwise onError is signalled with
     * {@link AerospikeException.BatchRecordArray} if batch save results contain errors, or with
     * {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Flux<T> saveAll(Iterable<T> documents);

    /**
     * Reactively save documents using one batch request with a given set name. The policies are analogous to
     * {@link #save(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents documents to save. Must not be {@literal null}.
     * @param setName   The set name to save to documents.
     * @return A Flux of the saved documents, otherwise onError is signalled with
     * {@link AerospikeException.BatchRecordArray} if batch save results contain errors, or with
     * {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Flux<T> saveAll(Iterable<T> documents, String setName);

    /**
     * Reactively insert document using {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to insert. Must not be {@literal null}.
     * @return A Mono of the new inserted document.
     */
    <T> Mono<T> insert(T document);

    /**
     * Reactively insert document with a given set name using
     * {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to insert. Must not be {@literal null}.
     * @param setName  The set name to insert the document.
     * @return A Mono of the new inserted document.
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
     * @return A Flux of the inserted documents, otherwise onError is signalled with
     * {@link AerospikeException.BatchRecordArray} if batch insert results contain errors, or with
     * {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Flux<T> insertAll(Iterable<? extends T> documents);

    /**
     * Reactively insert documents with a given set using one batch request. The policies are analogous to
     * {@link #insert(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents Documents to insert. Must not be {@literal null}.
     * @param setName   The set name to insert the documents.
     * @return A Flux of the inserted documents, otherwise onError is signalled with
     * {@link AerospikeException.BatchRecordArray} if batch insert results contain errors, or with
     * {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Flux<T> insertAll(Iterable<? extends T> documents, String setName);

    /**
     * Reactively persist a document using specified WritePolicy.
     *
     * @param document    The document to persist. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     * @return A Mono of the new persisted document.
     */
    <T> Mono<T> persist(T document, WritePolicy writePolicy);

    /**
     * Reactively persist a document using specified WritePolicy and a given set (overrides the set associated with the
     * document).
     *
     * @param document    The document to persist. Must not be {@literal null}.
     * @param writePolicy The Aerospike write policy for the inner Aerospike put operation. Must not be
     *                    {@literal null}.
     * @param setName     Set name to override the set associated with the document.
     * @return A Mono of the new persisted document.
     */
    <T> Mono<T> persist(T document, WritePolicy writePolicy, String setName);

    /**
     * Reactively update document using {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy
     * combined with removing bins at first (analogous to
     * {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into consideration the version
     * property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @return A Mono of the new updated document.
     */
    <T> Mono<T> update(T document);

    /**
     * Reactively update document with a given set using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy combined with removing bins at first
     * (analogous to {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY}) taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @param setName  The set name to update the document.
     * @return A Mono of the new updated document.
     */
    <T> Mono<T> update(T document, String setName);

    /**
     * Reactively update document specific fields based on a given collection of fields. using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy - You can instantiate the document with
     * only relevant fields and specify the list of fields that you want to update. taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @return A Mono of the new updated document.
     */
    <T> Mono<T> update(T document, Collection<String> fields);

    /**
     * Reactively update document specific fields based on a given collection of fields with a given set name. using
     * {@link com.aerospike.client.policy.RecordExistsAction#UPDATE_ONLY} policy - You can instantiate the document with
     * only relevant fields and specify the list of fields that you want to update. taking into consideration the
     * version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @param setName  The set name to update the document.
     * @return A Mono of the new updated document.
     */
    <T> Mono<T> update(T document, String setName, Collection<String> fields);

    /**
     * Reactively update documents using one batch request. The policies are analogous to {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents Documents to update. Must not be {@literal null}.
     * @return A Flux of the updated documents, otherwise onError is signalled with
     * {@link AerospikeException.BatchRecordArray} if batch update results contain errors, or with
     * {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Flux<T> updateAll(Iterable<? extends T> documents);

    /**
     * Reactively update documents using one batch request with a given set name. The policies are analogous to
     * {@link #update(Object)}.
     * <p>
     * The order of returned results is preserved. The execution order is NOT preserved.
     * <p>
     * Requires Server version 6.0+.
     *
     * @param documents Documents to update. Must not be {@literal null}.
     * @param setName   The set name to update the documents.
     * @return A Flux of the updated documents, otherwise onError is signalled with
     * {@link AerospikeException.BatchRecordArray} if batch update results contain errors, or with
     * {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Flux<T> updateAll(Iterable<? extends T> documents, String setName);

    /**
     * Reactively truncate/delete all the documents in the given entity's set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @deprecated since 4.6.0, use deleteAll(Class<T> entityClass) instead.
     */
    <T> Mono<Void> delete(Class<T> entityClass);

    /**
     * Reactively delete document by id, set name will be determined by the given entityClass.
     *
     * @param id          The id of the document to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of whether the document existed on server before deletion.
     * @deprecated since 4.6.0, use deleteById(Object id, Class<T> entityClass) instead.
     */
    <T> Mono<Boolean> delete(Object id, Class<T> entityClass);

    /**
     * Reactively delete document.
     *
     * @param document The document to delete. Must not be {@literal null}.
     * @return A Mono of whether the document existed on server before deletion.
     */
    <T> Mono<Boolean> delete(T document);

    /**
     * Reactively delete document with a given set name.
     *
     * @param document The document to delete. Must not be {@literal null}.
     * @param setName  Set name to delete the document.
     * @return A Mono of whether the document existed on server before deletion.
     */
    <T> Mono<Boolean> delete(T document, String setName);

    /**
     * Reactively delete document by id, set name will be determined by the given entityClass.
     *
     * @param id          The id of the document to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of whether the document existed on server before deletion.
     */
    <T> Mono<Boolean> deleteById(Object id, Class<T> entityClass);

    /**
     * Reactively delete document by id with a given set name.
     *
     * @param id      The id of the document to delete. Must not be {@literal null}.
     * @param setName Set name to delete the document.
     * @return A Mono of whether the document existed on server before deletion.
     */
    Mono<Boolean> deleteById(Object id, String setName);

    /**
     * Reactively delete documents by providing multiple ids using a single batch delete operation, set name will be
     * determined by the given entityClass.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return onError is signalled with {@link AerospikeException.BatchRecordArray} if batch delete results contain
     * errors, or with {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Mono<Void> deleteByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Reactively delete documents by providing multiple ids using a single batch delete operation with a given set
     * name.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to delete the document.
     * @return onError is signalled with {@link AerospikeException.BatchRecordArray} if batch delete results contain
     * errors, or with {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    Mono<Void> deleteByIds(Iterable<?> ids, String setName);

    /**
     * Reactively delete documents by providing multiple ids using a single batch delete operation, set name will be
     * determined by the given entityClass.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return onError is signalled with {@link AerospikeException.BatchRecordArray} if batch delete results contain
     * errors, or with {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    <T> Mono<Void> deleteByIds(Collection<?> ids, Class<T> entityClass);

    /**
     * Reactively delete documents by providing multiple ids using a single batch delete operation with a given set
     * name.
     * <p>
     * This operation requires Server version 6.0+.
     *
     * @param ids     The ids of the documents to find. Must not be {@literal null}.
     * @param setName Set name to delete the document.
     * @return onError is signalled with {@link AerospikeException.BatchRecordArray} if batch delete results contain
     * errors, or with {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    Mono<Void> deleteByIds(Collection<?> ids, String setName);

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
     * @return onError is signalled with {@link AerospikeException.BatchRecordArray} if batch delete results contain
     * errors, or with {@link org.springframework.dao.DataAccessException} if batch operation failed.
     */
    Mono<Void> deleteByIds(GroupedKeys groupedKeys);

    /**
     * Reactively truncate/delete all the documents in the given entity's set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     */
    <T> Mono<Void> deleteAll(Class<T> entityClass);

    /**
     * Reactively truncate/delete all the documents in the given entity's set.
     *
     * @param setName Set name to truncate/delete all the documents in.
     */
    Mono<Void> deleteAll(String setName);

    /**
     * Reactively add integer/double bin values to existing document bin values, read the new modified document and map
     * it back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param values   a Map of bin names and values to add. Must not be {@literal null}.
     * @return A Mono of the modified document after add operations.
     */
    <T> Mono<T> add(T document, Map<String, Long> values);

    /**
     * Reactively add integer/double bin values to existing document bin values with a given set name, read the new
     * modified document and map it back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param setName  Set name for the operation.
     * @param values   a Map of bin names and values to add. Must not be {@literal null}.
     * @return A Mono of the modified document after add operations.
     */
    <T> Mono<T> add(T document, String setName, Map<String, Long> values);

    /**
     * Reactively add integer/double bin value to existing document bin value, read the new modified document and map it
     * back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return A Mono of the modified document after add operation.
     */
    <T> Mono<T> add(T document, String binName, long value);

    /**
     * Reactively add integer/double bin value to existing document bin value with a given set name, read the new
     * modified document and map it back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param setName  Set name for the operation.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return A Mono of the modified document after add operation.
     */
    <T> Mono<T> add(T document, String setName, String binName, long value);

    /**
     * Reactively append bin string values to existing document bin values, read the new modified document and map it
     * back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param values   a Map of bin names and values to append. Must not be {@literal null}.
     * @return A Mono of the modified document after append operations.
     */
    <T> Mono<T> append(T document, Map<String, String> values);

    /**
     * Reactively append bin string values to existing document bin values with a given set name, read the new modified
     * document and map it back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param setName  Set name for the operation.
     * @param values   a Map of bin names and values to append. Must not be {@literal null}.
     * @return A Mono of the modified document after append operations.
     */
    <T> Mono<T> append(T document, String setName, Map<String, String> values);

    /**
     * Reactively append bin string value to existing document bin value, read the new modified document and map it back
     * the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return A Mono of the modified document after append operation.
     */
    <T> Mono<T> append(T document, String binName, String value);

    /**
     * Reactively append bin string value to existing document bin value with a given set name, read the new modified
     * document and map it back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use append operation on.
     * @param setName  Set name for the operation.
     * @param value    The value to append.
     * @return A Mono of the modified document after append operation.
     */
    <T> Mono<T> append(T document, String setName, String binName, String value);

    /**
     * Reactively prepend bin string values to existing document bin values, read the new modified document and map it
     * back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param values   a Map of bin names and values to prepend. Must not be {@literal null}.
     * @return A Mono of the modified document after prepend operations.
     */
    <T> Mono<T> prepend(T document, Map<String, String> values);

    /**
     * Reactively prepend bin string values to existing document bin values with a given set name, read the new modified
     * document and map it back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param setName  Set name for the operation.
     * @param values   a Map of bin names and values to prepend. Must not be {@literal null}.
     * @return A Mono of the modified document after prepend operations.
     */
    <T> Mono<T> prepend(T document, String setName, Map<String, String> values);

    /**
     * Reactively prepend bin string value to existing document bin value, read the new modified document and map it
     * back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return A Mono of the modified document after prepend operation.
     */
    <T> Mono<T> prepend(T document, String binName, String value);

    /**
     * Reactively prepend bin string value to existing document bin value with a given set name, read the new modified
     * document and map it back the given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be
     *                 {@literal null}.
     * @param binName  Bin name to use prepend operation on.
     * @param setName  Set name for the operation.
     * @param value    The value to prepend.
     * @return A Mono of the modified document after prepend operation.
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
     * Reactively find a document by id, set name will be determined by the given entityClass.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the document to. Must not be
     *                    {@literal null}.
     * @return A Mono of the matching document, returned document will be mapped to entityClass's type.
     */
    <T> Mono<T> findById(Object id, Class<T> entityClass);

    /**
     * Reactively find a document by id with a given set name.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to map the document to and to get entity properties from (such expiration). Must not
     *                    be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return A Mono of the matching document, returned document will be mapped to entityClass's type.
     */
    <T> Mono<T> findById(Object id, Class<T> entityClass, String setName);

    /**
     * Reactively find a document by id, set name will be determined by the given entityClass.
     * <p>
     * Document will be mapped to the given targetClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Mono of the matching document, returned document will be mapped to targetClass's type.
     */
    <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find a document by id with a given set name.
     * <p>
     * Document will be mapped to the given targetClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to map the document to and to get entity properties from (such expiration). Must not
     *                    be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @param setName     Set name to find the document from.
     * @return A Mono of the matching document, returned document will be mapped to targetClass's type.
     */
    <T, S> Mono<S> findById(Object id, Class<T> entityClass, Class<S> targetClass, String setName);

    /**
     * Reactively find documents by providing multiple ids using a single batch read operation, set name will be
     * determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Reactively find documents by providing multiple ids using a single batch read operation, set name will be
     * determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Flux<S> findByIds(Iterable<?> ids, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find documents by providing multiple ids using a single batch read operation with a given set name.
     * <p>
     * Documents will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     Set name to find the documents from.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> targetClass, String setName);

    /**
     * Reactively executes a single batch request to get results for several entities.
     * <p>
     * Aerospike provides functionality to get documents from different sets in 1 batch request. The methods allow to
     * put grouped keys by entity type as parameter and get result as spring data aerospike entities grouped by entity
     * type.
     *
     * @param groupedKeys Must not be {@literal null}.
     * @return Mono of grouped entities.
     */
    Mono<GroupedEntities> findByIds(GroupedKeys groupedKeys);

    /**
     * Find document by providing id, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The document from Aerospike, returned document will be mapped to targetClass's type.
     */
    <T, S> Mono<?> findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass,
                                      @Nullable Query query);

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
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The document from Aerospike, returned document will be mapped to targetClass's type.
     */
    <T, S> Mono<?> findByIdUsingQuery(Object id, Class<T> entityClass, Class<S> targetClass, String setName,
                                      @Nullable Query query);

    /**
     * Find documents by providing multiple ids, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given targetClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to.
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document
     * exists, an empty list is returned.
     */
    <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass,
                                       @Nullable Query query);

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
     * @param query       The {@link Query} to filter results. Optional argument (null if no filtering required).
     * @return The documents from Aerospike, returned documents will be mapped to targetClass's type, if no document
     * exists, an empty list is returned.
     */
    <T, S> Flux<?> findByIdsUsingQuery(Collection<?> ids, Class<T> entityClass, Class<S> targetClass, String setName,
                                       @Nullable Query query);

    /**
     * Reactively find documents in the given entityClass's set using a query and map them to the given class type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> find(Query query, Class<T> entityClass);

    /**
     * Reactively find documents in the given entityClass's set using a query and map them to the given target class
     * type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Flux<S> find(Query query, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find documents in the given set using a query and map them to the given target class type.
     *
     * @param query       The {@link Query} to filter results. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @param setName     Set name to find the documents from.
     * @return A Flux of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T> Flux<T> find(Query query, Class<T> targetClass, String setName);

    /**
     * Reactively find all documents in the given entityClass's set and map them to the given class type.
     *
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findAll(Class<T> entityClass);

    /**
     * Reactively find all documents in the given entityClass's set and map them to the given target class type.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Flux<S> findAll(Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find all documents in the given set and map them to the given class type.
     *
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     The set name to find the document.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findAll(Class<T> targetClass, String setName);

    /**
     * Reactively find all documents in the given entityClass's set using a provided sort and map them to the given
     * class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findAll(Sort sort, long offset, long limit, Class<T> entityClass);

    /**
     * Reactively find all documents in the given entityClass's set using a provided sort and map them to the given
     * target class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param entityClass The class to extract the Aerospike set from.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Flux<S> findAll(Sort sort, long offset, long limit, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively find all documents in the given entityClass's set using a provided sort and map them to the given
     * target class type.
     *
     * @param sort        The sort to affect the returned iterable documents order.
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param targetClass The class to map the documents to. Must not be {@literal null}.
     * @param setName     The set name to find the documents.
     * @return A Flux of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T> Flux<T> findAll(Sort sort, long offset, long limit, Class<T> targetClass, String setName);

    /**
     * Reactively find documents in the given entityClass's set using a range (offset, limit) and a sort and map them to
     * the given class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the order of the returned Stream of documents.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be
     *                    {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

    /**
     * Reactively find documents in the given set using a range (offset, limit) and a sort and map them to the given
     * class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the order of the returned Stream of documents.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @param setName     Set name to find the documents from.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> targetClass, String setName);

    /**
     * Reactively find documents in the given entityClass's set using a range (offset, limit) and a sort and map them to
     * the given target class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the returned Stream of documents order.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param targetClass The class to map the document to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to targetClass's type.
     */
    <T, S> Flux<S> findInRange(long offset, long limit, Sort sort, Class<T> entityClass, Class<S> targetClass);

    /**
     * Reactively check if document exists by providing document id and entityClass (set name will be determined by the
     * given entityClass).
     *
     * @param id          The id to check if exists. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of whether the document exists.
     */
    <T> Mono<Boolean> exists(Object id, Class<T> entityClass);

    /**
     * Reactively check if document exists by providing document id with a given set name.
     *
     * @param id      The id to check if exists. Must not be {@literal null}.
     * @param setName Set name to check if document exists.
     * @return A Mono of whether the document exists.
     */
    Mono<Boolean> exists(Object id, String setName);

    /**
     * Reactively check if any document exists by defining a query and entityClass (set name will be determined by the
     * given entityClass).
     *
     * @param query       The query to check if any returned document exists. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of whether the document exists.
     */
    <T> Mono<Boolean> existsByQuery(Query query, Class<T> entityClass);

    /**
     * Reactively check if any document exists by defining a query, entityClass and a given set name.
     *
     * @param query       The query to check if any returned document exists. Must not be {@literal null}.
     * @param entityClass The class to translate to returned results into. Must not be {@literal null}.
     * @param setName     The set name to check if documents exists in. Must not be {@literal null}.
     * @return A Mono of whether the document exists.
     */
    <T> Mono<Boolean> existsByQuery(Query query, Class<T> entityClass, String setName);

    /**
     * Reactively return the amount of documents in the given entityClass's Aerospike set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of the amount of documents in the set (of the given entityClass).
     */
    <T> Mono<Long> count(Class<T> entityClass);

    /**
     * Reactively return the amount of documents in the given Aerospike set.
     *
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return A Mono of the amount of documents in the given set.
     */
    Mono<Long> count(String setName);

    /**
     * Reactively return the amount of documents in a query results. set name will be determined by the given
     * entityClass.
     *
     * @param query       The query that provides the result set for count.
     * @param entityClass entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of the amount of documents that the given query and entity class supplied.
     */
    <T> Mono<Long> count(Query query, Class<T> entityClass);

    /**
     * Reactively return the amount of documents in a query results with a given set name.
     *
     * @param query   The query that provides the result set for count.
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return A Mono of the amount of documents that the given query and set name.
     */
    Mono<Long> count(Query query, String setName);

    /**
     * Reactively create index by specified name in Aerospike.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     * @param binName     The bin name to create the index on. Must not be {@literal null}.
     * @param indexType   The type of the index. Must not be {@literal null}.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName,
                               String binName, IndexType indexType);

    /**
     * Reactively create index by specified name in Aerospike.
     *
     * @param entityClass         The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName, String binName,
                               IndexType indexType, IndexCollectionType indexCollectionType);

    /**
     * Reactively create index by specified name in Aerospike.
     *
     * @param entityClass         The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     * @param ctx                 optional context to index on elements within a CDT.
     */
    <T> Mono<Void> createIndex(Class<T> entityClass, String indexName, String binName,
                               IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx);

    /**
     * Reactively create index by specified name in Aerospike.
     *
     * @param setName   Set name to create the index.
     * @param indexName The index name. Must not be {@literal null}.
     * @param binName   The bin name to create the index on. Must not be {@literal null}.
     * @param indexType The type of the index. Must not be {@literal null}.
     */
    Mono<Void> createIndex(String setName, String indexName,
                           String binName, IndexType indexType);

    /**
     * Reactively create index by specified name in Aerospike.
     *
     * @param setName             Set name to create the index.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     */
    Mono<Void> createIndex(String setName, String indexName, String binName,
                           IndexType indexType, IndexCollectionType indexCollectionType);

    /**
     * Reactively create index by specified name in Aerospike.
     *
     * @param setName             Set name to create the index.
     * @param indexName           The index name. Must not be {@literal null}.
     * @param binName             The bin name to create the index on. Must not be {@literal null}.
     * @param indexType           The type of the index. Must not be {@literal null}.
     * @param indexCollectionType The collection type of the index. Must not be {@literal null}.
     * @param ctx                 optional context to index on elements within a CDT.
     */
    Mono<Void> createIndex(String setName, String indexName, String binName,
                           IndexType indexType, IndexCollectionType indexCollectionType, CTX... ctx);

    /**
     * Reactively delete index by specified name from Aerospike.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     */
    <T> Mono<Void> deleteIndex(Class<T> entityClass, String indexName);

    /**
     * Reactively delete index by specified name from Aerospike.
     *
     * @param setName   Set name to delete the index from.
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
