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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;

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
	 * The Set name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	<T> String getSetName(Class<T> entityClass);
	
	/**
	 * Insert operation using {@link com.aerospike.client.policy.RecordExistsAction#CREATE_ONLY} policy.
	 *
	 * If document has version property it will be updated with the server's version after successful operation.
	 * @param document
	 */
	<T> void insert(T document);

	/**
	 * @return mapping context in use.
	 */
	MappingContext<?, ?> getMappingContext();
	
	/**
	 * Save operation.
	 *
	 * If document has version property - CAS algorithm is used for updating record.
	 * Version property is used for deciding whether to create new record or update existing.
	 * If version is set to zero - new record will be created, creation will fail is such record already exists.
	 * If version is greater than zero - existing record will be updated with {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY} policy
	 * taking into consideration the version property of the document.
	 * Version property will be updated with the server's version after successful operation.
	 *
	 * If document does not have version property - record is updated with {@link com.aerospike.client.policy.RecordExistsAction#REPLACE} policy.
	 * This means that when such record does not exist it will be created, otherwise updated.
	 * @param document
	 */
	<T> void save(T document);

	/**
	 * Persist document using specified WritePolicy
	 * @param document
	 * @param writePolicy
	 */
	<T> void persist(T document, WritePolicy writePolicy);

	/**
	 * Update operation using {@link com.aerospike.client.policy.RecordExistsAction#REPLACE_ONLY} policy
	 * taking into consideration the version property of the document if it is present.
	 *
	 * If document has version property it will be updated with the server's version after successful operation.
	 * @param objectToUpdate
	 */
	<T> void update(T objectToUpdate);

	<T> void delete(Class<T> entityClass);

	<T> boolean delete(Object id, Class<T> entityClass);
	
	<T> boolean delete(T objectToDelete);

	<T> boolean exists(Object id, Class<T> entityClass);
	
	<T> Stream<T> find(Query query, Class<T> entityClass);
	
	<T> Stream<T> findAll(Class<T> entityClass);

	<T> T findById(Object id, Class<T> entityClass);

	<T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass);

	<T> T add(T objectToAddTo, Map<String, Long> values);
	
	<T> T add(T objectToAddTo, String binName, long value);

	<T> T append(T objectToAppendTo, Map<String, String> values);
	
	<T> T append(T objectToAppendTo, String binName, String value);
	
	<T> T prepend(T objectToPrependTo, Map<String, String> values);
	
	<T> T prepend(T objectToPrependTo, String binName, String value);
	
	<T> Iterable<T> aggregate(Filter filter, Class<T> entityClass, String module, String function, List<Value> arguments);

	/**
	 * @param query
	 * @param entityClass
	 * @return
	 */
	<T> long count(Query query, Class<T> entityClass);

	/**
	 * Execute operation against underlying store.
	 * 
	 * @param supplier must not be {@literal null}.
	 * @return
	 */
	<T> T execute(Supplier<T> supplier);

	/**
	 * @param sort
	 * @param entityClass
	 * @return
	 */
	<T> Iterable<T> findAll(Sort sort, Class<T> entityClass);

	/**
	 * @param offset
	 * @param limit
	 * @param sort
	 * @param entityClass
	 * @return
	 */
	<T> Stream<T> findInRange(long offset, long limit, Sort sort, Class<T> entityClass);

	/**
	 * @param entityClass
	 * @return
	 */
	<T> long count(Class<T> entityClass, String setName);

	/**
	 * Creates index by specified name in Aerospike.
	 */
	<T> void createIndex(Class<T> entityClass, String indexName, String binName,
			IndexType indexType);

	/**
	 * Creates index by specified name in Aerospike.
	 */
	<T> void createIndex(Class<T> entityClass, String indexName, String binName,
						 IndexType indexType, IndexCollectionType indexCollectionType);

	/**
	 * Deletes index by specified name from Aerospike.
	 * @param entityClass
	 * @param indexName
	 */
	<T> void deleteIndex(Class<T> entityClass, String indexName);

	/**
	 * Checks whether index by specified name exists in Aerospike.
	 * @param indexName
	 * @return true if exists
	 * @deprecated This operation is deprecated due to complications that are required for guaranteed index existence response.
	 * <p>If you need to conditionally create index \u2014 replace {@link #indexExists} with {@link #createIndex} and catch {@link IndexAlreadyExistsException}.
	 * <p>More information can be found at: <a href="https://github.com/aerospike/aerospike-client-java/pull/149">https://github.com/aerospike/aerospike-client-java/pull/149</a>
	 */
	@Deprecated
	boolean indexExists(String indexName);

	/**
	 * @param entityClass
	 * @return
	 */
	<T> long count(Class<T> entityClass);

	AerospikeClient getAerospikeClient();
}
