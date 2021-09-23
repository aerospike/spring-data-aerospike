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
     * If document has version property - CAS algorithm is used for updating record.
     * Version property is used for deciding whether to create new record or update existing.
     * If version is set to zero - new record will be created, creation will fail is such record already exists.
     * If version is greater than zero - existing record will be updated with {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY} policy
     * taking into consideration the version property of the document.
     * Version property will be updated with the server's version after successful operation.
     * <p>
     * If document does not have version property - record is updated with {@link com.aerospike.client.policy.RecordExistsAction#REPLACE} policy.
     * This means that when such record does not exist it will be created, otherwise updated.
     *
     * @param document The document to save. Must not be {@literal null}.
     * @return A Mono of the new saved document.
     */
    <T> Mono<T> save(T document);

    /**
     * Reactively insert each document of the given documents using single insert operations.
     *
     * @param documents The documents to insert. Must not be {@literal null}.
     * @return A Flux of the new inserted documents.
     */
    <T> Flux<T> insertAll(Collection<? extends T> documents);

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
     * Reactively update document using {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY} policy
     * taking into consideration the version property of the document if it is present.
     * <p>
     * If document has version property it will be updated with the server's version after successful operation.
     *
     * @param document The document to update. Must not be {@literal null}.
     * @return A Mono of the new updated document.
     */
    <T> Mono<T> update(T document);

    /**
     * Reactively add integer/double bin values to existing document bin values, read the new modified document and map it back the the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param values   a Map of bin names and values to add. Must not be {@literal null}.
     * @return A Mono of the modified document after add operations.
     */
    <T> Mono<T> add(T document, Map<String, Long> values);

    /**
     * Reactively add integer/double bin value to existing document bin value, read the new modified document and map it back the the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param binName  Bin name to use add operation on. Must not be {@literal null}.
     * @param value    The value to add.
     * @return A Mono of the modified document after add operation.
     */
    <T> Mono<T> add(T document, String binName, long value);

    /**
     * Reactively append bin string values to existing document bin values, read the new modified document and map it back the the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param values   a Map of bin names and values to append. Must not be {@literal null}.
     * @return A Mono of the modified document after append operations.
     */
    <T> Mono<T> append(T document, Map<String, String> values);

    /**
     * Reactively append bin string value to existing document bin value, read the new modified document and map it back the the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param binName  Bin name to use append operation on.
     * @param value    The value to append.
     * @return A Mono of the modified document after append operation.
     */
    <T> Mono<T> append(T document, String binName, String value);

    /**
     * Reactively prepend bin string values to existing document bin values, read the new modified document and map it back the the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param values   a Map of bin names and values to prepend. Must not be {@literal null}.
     * @return A Mono of the modified document after prepend operations.
     */
    <T> Mono<T> prepend(T document, Map<String, String> values);

    /**
     * Reactively prepend bin string value to existing document bin value, read the new modified document and map it back the the
     * given document class type.
     *
     * @param document The document to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @param binName  Bin name to use prepend operation on.
     * @param value    The value to prepend.
     * @return A Mono of the modified document after prepend operation.
     */
    <T> Mono<T> prepend(T document, String binName, String value);

    /**
     * Reactively find all documents in the given entityClass's set and map them to the given class type.
     *
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findAll(Class<T> entityClass);

    /**
     * Reactively find a document by id, set name will be determined by the given entityClass.
     * <p>
     * Document will be mapped to the given entityClass.
     *
     * @param id          The id of the document to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the document to. Must not be {@literal null}.
     * @return A Mono of the matching document, returned document will be mapped to entityClass's type.
     */
    <T> Mono<T> findById(Object id, Class<T> entityClass);

    /**
     * Reactively find documents by providing multiple ids using a single batch read operation, set name will be determined by the given entityClass.
     * <p>
     * Documents will be mapped to the given entityClass.
     *
     * @param ids         The ids of the documents to find. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findByIds(Iterable<?> ids, Class<T> entityClass);

    /**
     * Reactively executes a single batch request to get results for several entities.
     * <p>
     * Aerospike provides functionality to get documents from different sets in 1 batch
     * request. The methods allows to put grouped keys by entity type as parameter and
     * get result as spring data aerospike entities grouped by entity type.
     *
     * @param groupedKeys Must not be {@literal null}.
     * @return Mono of grouped entities.
     */
    Mono<GroupedEntities> findByIds(GroupedKeys groupedKeys);

    /**
     * Reactively find documents in the given entityClass's set using a query and map them to the given class type.
     *
     * @param query       The query to filter results. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> find(Query query, Class<T> entityClass);

    /**
     * Reactively find documents in the given entityClass's set using a range (offset, limit) and a sort
     * and map them to the given class type.
     *
     * @param offset      The offset to start the range from.
     * @param limit       The limit of the range.
     * @param sort        The sort to affect the returned Stream of documents order.
     * @param entityClass The class to extract the Aerospike set from and to map the documents to. Must not be {@literal null}.
     * @return A Flux of matching documents, returned documents will be mapped to entityClass's type.
     */
    <T> Flux<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

    /**
     * Reactively return the amount of documents in a query results. set name will be determined by the given entityClass.
     *
     * @param query       The query that provides the result set for count.
     * @param entityClass entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of the amount of documents that the given query and entity class supplied.
     */
    <T> Mono<Long> count(Query query, Class<T> entityClass);

    /**
     * Reactively return the amount of documents in the given Aerospike set.
     *
     * @param setName The name of the set to count. Must not be {@literal null}.
     * @return A Mono of the amount of documents in the given set.
     */
    Mono<Long> count(String setName);

    /**
     * Reactively return the amount of documents in the given entityClass's Aerospike set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of the amount of documents in the set (of the given entityClass).
     */
    <T> Mono<Long> count(Class<T> entityClass);

    /**
     * Reactively execute operation against underlying store.
     *
     * @param supplier must not be {@literal null}.
     * @return A Mono of the execution result.
     */
    <T> Mono<T> execute(Supplier<T> supplier);

    /**
     * Reactively check if document exists by providing document id and entityClass (set name will be determined by the given entityClass).
     *
     * @param id          The id to check if exists. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of whether the document exists.
     */
    <T> Mono<Boolean> exists(Object id, Class<T> entityClass);

    /**
     * Reactively truncate/delete all the documents in the given entity's set.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     */
    <T> Mono<Void> delete(Class<T> entityClass);

    /**
     * Reactively delete document by id, set name will be determined by the given entityClass.
     *
     * @param id          The id of the document to delete. Must not be {@literal null}.
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @return A Mono of whether the document existed on server before deletion.
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
     * Reactively delete index by specified name from Aerospike.
     *
     * @param entityClass The class to extract the Aerospike set from. Must not be {@literal null}.
     * @param indexName   The index name. Must not be {@literal null}.
     */
    <T> Mono<Void> deleteIndex(Class<T> entityClass, String indexName);
}
