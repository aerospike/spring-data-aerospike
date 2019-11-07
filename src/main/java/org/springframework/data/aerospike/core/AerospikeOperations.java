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

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Value;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;

/**
 * Aerospike specific data access operations.
 * 
 * @author Oliver Gierke
 * @author Peter Milne
 */
public interface AerospikeOperations {//extends KeyValueOperations {

	/**
	 * The Set name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getSetName(Class<?> entityClass);
	
	/**
	 * Insert operation using the WritePolicy.recordExisits policy of CREATE_ONLY 
	 * @param document
	 */
	void insert(Object document);

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
	 * If version is greater than zero - existing record will be updated with RecordExistsAction.REPLACE_ONLY policy
	 * taking into consideration the version property of the document.
	 *
	 * If document does not have version property - record is updated with RecordExistsAction.UPDATE policy.
	 * This means that when such record does not exist it will be created, otherwise updated.
	 * @param document
	 */
	void save(Object document);

	/**
	 * Persist document using specified WritePolicy
	 * @param document
	 * @param writePolicy
	 */
	void persist(Object document, WritePolicy writePolicy);

	/**
	 * Update operation using the WritePolicy.recordExisits policy of UPDATE_ONLY
	 * @param objectToUpdate
	 */
	void update(Object objectToUpdate);

	void delete(Class<?> type);

	boolean delete(Object id, Class<?> type);
	boolean delete(Object objectToDelete);

	boolean exists(Object id, Class<?> type);
	
	<T> Stream<T> find(Query query, Class<T> type);
	<T> Stream<T> findAll(Class<T> type);

	<T> T findById(Object id, Class<T> type);

	<T> List<T> findByIds(Iterable<?> IDs, Class<T> type);

	<T> T add(T objectToAddTo, Map<String, Long> values);
	<T> T add(T objectToAddTo, String binName, long value);

	<T> T append(T objectToAppendTo, Map<String, String> values);
	<T> T append(T objectToAppendTo, String binName, String value);
	<T> T prepend(T objectToPrependTo, Map<String, String> values);
	<T> T prepend(T objectToPrependTo, String binName, String value);
	
	<T> Iterable<T> aggregate(Filter filter, Class<T> outputType, String module, String function, List<Value> arguments);



	/**
	 * @param query
	 * @param javaType
	 * @return
	 */
	long count(Query query, Class<?> javaType);

	/**
	 * Execute operation against underlying store.
	 * 
	 * @param supplier must not be {@literal null}.
	 * @return
	 */
	<T> T execute(Supplier<T> supplier);

	/**
	 * @param sort
	 * @param type
	 * @return
	 */
	<T> Iterable<T> findAll(Sort sort, Class<T> type);

	/**
	 * @param offset
	 * @param limit
	 * @param sort
	 * @param type
	 * @return
	 */
	<T> Stream<T> findInRange(long offset, long limit, Sort sort, Class<T> type);

	/**
	 * @param type
	 * @return
	 */
	long count(Class<?> type,String setName);

	/**
	 * Creates index by specified name in Aerospike.
	 * @param domainType
	 * @param indexName
	 * @param binName
	 * @param indexType
	 */
	<T> void createIndex(Class<T> domainType, String indexName, String binName,
			IndexType indexType);

	/**
	 * Deletes index by specified name from Aerospike.
	 * @param domainType
	 * @param indexName
	 */
	<T> void deleteIndex(Class<T> domainType, String indexName);

	/**
	 * Checks whether index by specified name exists in Aerospike.
	 * @param indexName
	 * @return true if exists
	 */
	boolean indexExists(String indexName);

	/**
	 * @param type
	 * @return
	 */
	long count(Class<?> type);

	AerospikeClient getAerospikeClient();
}
