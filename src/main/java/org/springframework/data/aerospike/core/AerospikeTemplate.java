/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import com.aerospike.client.Record;
import com.aerospike.client.*;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.task.IndexTask;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.model.GroupedEntities;
import org.springframework.data.aerospike.core.model.GroupedKeys;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.query.KeyRecordIterator;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.cache.IndexRefresher;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.aerospike.utility.Utils;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.util.StreamUtils;
import org.springframework.util.Assert;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.springframework.data.aerospike.core.OperationUtils.operations;

/**
 * Primary implementation of {@link AerospikeOperations}.
 *
 * @author Oliver Gierke
 * @author Peter Milne
 * @author Anastasiia Smirnova
 * @author Igor Ermolenko
 * @author Roman Terentiev
 */
@Slf4j
public class AerospikeTemplate extends BaseAerospikeTemplate implements AerospikeOperations {

	private final IAerospikeClient client;
	private final QueryEngine queryEngine;
	private final IndexRefresher indexRefresher;

	public AerospikeTemplate(IAerospikeClient client,
							 String namespace,
							 MappingAerospikeConverter converter,
							 AerospikeMappingContext mappingContext,
							 AerospikeExceptionTranslator exceptionTranslator,
							 QueryEngine queryEngine,
							 IndexRefresher indexRefresher) {
		super(namespace, converter, mappingContext, exceptionTranslator, client.getWritePolicyDefault());
		this.client = client;
		this.queryEngine = queryEngine;
		this.indexRefresher = indexRefresher;
	}

	@Override
	public <T> void createIndex(Class<T> entityClass, String indexName,
								String binName, IndexType indexType) {
		createIndex(entityClass, indexName, binName, indexType, IndexCollectionType.DEFAULT);
	}

	@Override
	public <T> void createIndex(Class<T> entityClass, String indexName,
								String binName, IndexType indexType, IndexCollectionType indexCollectionType) {
		Assert.notNull(entityClass, "Type must not be null!");
		Assert.notNull(indexName, "Index name must not be null!");
		Assert.notNull(binName, "Bin name must not be null!");
		Assert.notNull(indexType, "Index type must not be null!");
		Assert.notNull(indexCollectionType, "Index collection type must not be null!");

		try {
			String setName = getSetName(entityClass);
			IndexTask task = client.createIndex(null, this.namespace,
					setName, indexName, binName, indexType, indexCollectionType);
			if (task != null) {
				task.waitTillComplete();
			}
			indexRefresher.refreshIndexes();
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> void deleteIndex(Class<T> entityClass, String indexName) {
		Assert.notNull(entityClass, "Type must not be null!");
		Assert.notNull(indexName, "Index name must not be null!");

		try {
			String setName = getSetName(entityClass);
			IndexTask task = client.dropIndex(null, this.namespace, setName, indexName);
			if (task != null) {
				task.waitTillComplete();
			}
			indexRefresher.refreshIndexes();
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public boolean indexExists(String indexName) {
		Assert.notNull(indexName, "Index name must not be null!");
		log.warn("`indexExists` operation is deprecated. Please stop using it as it will be removed in next major release.");

		try {
			Node[] nodes = client.getNodes();
			Node node = Utils.getRandomNode(nodes);
			String response = Info.request(node, "sindex/" + namespace + '/' + indexName);
			return !response.startsWith("FAIL:201");
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> void save(T document) {
		Assert.notNull(document, "Document must not be null!");

		AerospikeWriteData data = writeData(document);

		AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
		if (entity.hasVersionProperty()) {
			WritePolicy policy = expectGenerationCasAwareSavePolicy(data);

			doPersistWithVersionAndHandleCasError(document, data, policy);
		} else {
			WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.REPLACE);

			doPersistAndHandleError(data, policy);
		}
	}

	@Override
	public <T> void persist(T document, WritePolicy policy) {
		Assert.notNull(document, "Document must not be null!");
		Assert.notNull(policy, "Policy must not be null!");

		AerospikeWriteData data = writeData(document);

		doPersistAndHandleError(data, policy);
	}

	@Override
	public <T> void insertAll(Collection<? extends T> documents) {
		Assert.notNull(documents, "Documents must not be null!");

		documents.stream().filter(Objects::nonNull).forEach(this::insert);
	}

	@Override
	public <T> void insert(T document) {
		Assert.notNull(document, "Document must not be null!");

		AerospikeWriteData data = writeData(document);
		WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.CREATE_ONLY);
		AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
		if (entity.hasVersionProperty()) {
			// we are ignoring generation here as insert operation should fail with DuplicateKeyException if key already exists
			// and we do not mind which initial version is set in the document, BUT we need to update the version value in the original document
			// also we do not want to handle aerospike error codes as cas aware error codes as we are ignoring generation
			doPersistWithVersionAndHandleError(document, data, policy);
		} else {
			doPersistAndHandleError(data, policy);
		}
	}

	@Override
	public <T> void update(T document) {
		Assert.notNull(document, "Document must not be null!");

		AerospikeWriteData data = writeData(document);
		AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());
		if (entity.hasVersionProperty()) {
			WritePolicy policy = expectGenerationSavePolicy(data, RecordExistsAction.REPLACE_ONLY);

			doPersistWithVersionAndHandleCasError(document, data, policy);
		} else {
			WritePolicy policy = ignoreGenerationSavePolicy(data, RecordExistsAction.REPLACE_ONLY);

			doPersistAndHandleError(data, policy);
		}
	}

	@Override
	public <T> void delete(Class<T> entityClass) {
		Assert.notNull(entityClass, "Type must not be null!");

		try {
			String set = getSetName(entityClass);
			client.truncate(null, getNamespace(), set, null);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> boolean delete(Object id, Class<T> entityClass) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(entityClass, "Type must not be null!");

		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
			Key key = getKey(id, entity);

			return this.client.delete(ignoreGenerationDeletePolicy(), key);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> boolean delete(T document) {
		Assert.notNull(document, "Document must not be null!");

		try {
			AerospikeWriteData data = writeData(document);

			return this.client.delete(ignoreGenerationDeletePolicy(), data.getKey());
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> boolean exists(Object id, Class<T> entityClass) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(entityClass, "Type must not be null!");

		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
			Key key = getKey(id, entity);

			Record aeroRecord = this.client.operate(null, key, Operation.getHeader());
			return aeroRecord != null;
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> Stream<T> findAll(Class<T> entityClass) {
		Assert.notNull(entityClass, "Type must not be null!");

		return findAllUsingQuery(entityClass, null, (Qualifier[])null);
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(entityClass, "Type must not be null!");

		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);
			Key key = getKey(id, entity);

			Record aeroRecord;
			if (entity.isTouchOnRead()) {
				Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
				aeroRecord = getAndTouch(key, entity.getExpiration());
			} else {
				aeroRecord = this.client.get(null, key);
			}

			return mapToEntity(key, entityClass, aeroRecord);
		}
		catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	private Record getAndTouch(Key key, int expiration) {
		WritePolicy writePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
				.expiration(expiration)
				.build();

		if (this.client.exists(null, key)) {
			return this.client.operate(writePolicy, key, Operation.touch(), Operation.get());
		}

		return null;
	}

	@Override
	public <T> List<T> findByIds(Iterable<?> ids, Class<T> entityClass) {
		Assert.notNull(ids, "List of ids must not be null!");
		Assert.notNull(entityClass, "Type must not be null!");

		return findByIdsInternal(IterableConverter.toList(ids), entityClass);
	}

	private <T> List<T> findByIdsInternal(Collection<?> ids, Class<T> entityClass) {
		if (ids.isEmpty()) {
			return Collections.emptyList();
		}

		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

			Key[] keys = ids.stream()
					.map(id -> getKey(id, entity))
					.toArray(Key[]::new);

			Record[] aeroRecords = client.get(null, keys);

			return IntStream.range(0, keys.length)
					.filter(index -> aeroRecords[index] != null)
					.mapToObj(index -> mapToEntity(keys[index], entityClass, aeroRecords[index]))
					.collect(Collectors.toList());
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public GroupedEntities findByIds(GroupedKeys groupedKeys) {
		Assert.notNull(groupedKeys, "Grouped keys must not be null!");

		if (groupedKeys.getEntitiesKeys().isEmpty()) {
			return GroupedEntities.builder().build();
		}

		return findEntitiesByIdsInternal(groupedKeys);
	}

	private GroupedEntities findEntitiesByIdsInternal(GroupedKeys groupedKeys) {
		EntitiesKeys entitiesKeys = EntitiesKeys.of(toEntitiesKeyMap(groupedKeys));
		Record[] aeroRecords = client.get(null, entitiesKeys.getKeys());

		return toGroupedEntities(entitiesKeys, aeroRecords);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Iterable<T> aggregate(Filter filter, Class<T> entityClass,
			String module, String function, List<Value> arguments) {
		Assert.notNull(entityClass, "Type must not be null!");

		AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(entityClass);

		Statement statement = new Statement();
		if (filter != null)
			statement.setFilter(filter);
		statement.setSetName(entity.getSetName());
		statement.setNamespace(this.namespace);
		ResultSet resultSet;
		if (arguments != null && arguments.size() > 0)
			resultSet = this.client.queryAggregate(null, statement, module,
					function, arguments.toArray(new Value[0]));
		else
			resultSet = this.client.queryAggregate(null, statement);
		return (Iterable<T>) resultSet;
	}

	@Override
	public <T> Iterable<T> findAll(Sort sort, Class<T> entityClass) {
		throw new UnsupportedOperationException("not implemented");
	}

	public <T> boolean exists(Query query, Class<T> entityClass) {
		Assert.notNull(query, "Query passed in to exist can't be null");
		Assert.notNull(entityClass, "Type must not be null!");

		return find(query, entityClass).findAny().isPresent();
	}

	@Override
	public <T> T execute(Supplier<T> supplier) {
		Assert.notNull(supplier, "Supplier must not be null!");

		try {
			return supplier.get();
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> long count(Query query, Class<T> entityClass) {
		Assert.notNull(entityClass, "Type must not be null!");

		Stream<KeyRecord> results = findAllRecordsUsingQuery(entityClass, query);
		return results.count();
	}

	@Override
	public <T> Stream<T> find(Query query, Class<T> entityClass) {
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(entityClass, "Type must not be null!");

		return findAllUsingQuery(entityClass, query);
	}

	@Override
	public <T> Stream<T> findInRange(long offset, long limit, Sort sort,
									 Class<T> entityClass) {
		Assert.notNull(entityClass, "Type for count must not be null!");

		Stream<T> results = findAllUsingQuery(entityClass, null, (Qualifier[])null);
		//TODO:create a sort
		return results.skip(offset).limit(limit);
	}

	@Override
	public <T> long count(Class<T> entityClass) {
		Assert.notNull(entityClass, "Type for count must not be null!");

		String setName = getSetName(entityClass);
		return count(setName);
	}

	@Override
	public IAerospikeClient getAerospikeClient() {
		return client;
	}

	@Override
	public long count(String setName) {
		Assert.notNull(setName, "Set for count must not be null!");

		try {
			Node[] nodes = client.getNodes();

			int replicationFactor = Utils.getReplicationFactor(nodes, namespace);

			long totalObjects = Arrays.stream(nodes)
					.mapToLong(node -> Utils.getObjectsCount(node, namespace, setName))
					.sum();

			return (nodes.length > 1) ? (totalObjects / replicationFactor) : totalObjects;
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> T prepend(T document, String fieldName, String value) {
		Assert.notNull(document, "Document must not be null!");

		try {
			AerospikeWriteData data = writeData(document);
			Record aeroRecord = this.client.operate(null, data.getKey(),
					Operation.prepend(new Bin(fieldName, value)),
					Operation.get(fieldName));

			return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> T prepend(T document, Map<String, String> values) {
		Assert.notNull(document, "Document must not be null!");
		Assert.notNull(values, "Values must not be null!");

		try {
			AerospikeWriteData data = writeData(document);
			Operation[] ops = operations(values, Operation.Type.PREPEND, Operation.get());
			Record aeroRecord = this.client.operate(null, data.getKey(), ops);

			return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
		}
		catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> T append(T document, Map<String, String> values) {
		Assert.notNull(document, "Document must not be null!");
		Assert.notNull(values, "Values must not be null!");

		try {
			AerospikeWriteData data = writeData(document);
			Operation[] ops = operations(values, Operation.Type.APPEND, Operation.get());
			Record aeroRecord = this.client.operate(null, data.getKey(), ops);

			return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
		}
		catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> T append(T document, String binName, String value) {
		Assert.notNull(document, "Document must not be null!");

		try {

			AerospikeWriteData data = writeData(document);
			Record aeroRecord = this.client.operate(null, data.getKey(),
					Operation.append(new Bin(binName, value)),
					Operation.get(binName));

			return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> T add(T document, Map<String, Long> values) {
		Assert.notNull(document, "Document must not be null!");
		Assert.notNull(values, "Values must not be null!");

		try {
			AerospikeWriteData data = writeData(document);
			Operation[] ops = operations(values, Operation.Type.ADD, Operation.get());

			WritePolicy writePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
					.expiration(data.getExpiration())
					.build();

			Record aeroRecord = this.client.operate(writePolicy, data.getKey(), ops);

			return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	@Override
	public <T> T add(T document, String binName, long value) {
		Assert.notNull(document, "Document must not be null!");
		Assert.notNull(binName, "Bin name must not be null!");

		try {
			AerospikeWriteData data = writeData(document);

			WritePolicy writePolicy = WritePolicyBuilder.builder(client.getWritePolicyDefault())
					.expiration(data.getExpiration())
					.build();

			Record aeroRecord = this.client.operate(writePolicy, data.getKey(),
					Operation.add(new Bin(binName, value)), Operation.get());

			return mapToEntity(data.getKey(), getEntityClass(document), aeroRecord);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	private void doPersistAndHandleError(AerospikeWriteData data, WritePolicy policy) {
		try {
			put(data, policy);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	private <T> void doPersistWithVersionAndHandleCasError(T document, AerospikeWriteData data, WritePolicy policy) {
		try {
			Record newAeroRecord = putAndGetHeader(data, policy);
			updateVersion(document, newAeroRecord);
		} catch (AerospikeException e) {
			throw translateCasError(e);
		}
	}

	private <T> void doPersistWithVersionAndHandleError(T document, AerospikeWriteData data, WritePolicy policy) {
		try {
			Record newAeroRecord = putAndGetHeader(data, policy);
			updateVersion(document, newAeroRecord);
		} catch (AerospikeException e) {
			throw translateError(e);
		}
	}

	private void put(AerospikeWriteData data, WritePolicy policy) {
		Key key = data.getKey();
		Bin[] bins = data.getBinsAsArray();

		client.put(policy, key, bins);
	}

	private Record putAndGetHeader(AerospikeWriteData data, WritePolicy policy) {
		Key key = data.getKey();
		Bin[] bins = data.getBinsAsArray();

		if (bins.length == 0) {
			throw new AerospikeException("Cannot put and get header on a document with no bins and \"@_class\" bin disabled.");
		}

		Operation[] operations = operations(bins, Operation::put, Operation.getHeader());

		return client.operate(policy, key, operations);
	}

	<T> Stream<T> findAllUsingQuery(Class<T> type, Query query) {
		if ((query.getSort() == null || query.getSort().isUnsorted())
				&& query.getOffset() > 0) {
			throw new IllegalArgumentException("Unsorted query must not have offset value. " +
					"For retrieving paged results use sorted query.");
		}

		Qualifier qualifier = query.getCriteria().getCriteriaObject();
		Stream<T> results = findAllUsingQuery(type, null, qualifier);

		if (query.getSort() != null && query.getSort().isSorted()) {
			Comparator<T> comparator = getComparator(query);
			results = results.sorted(comparator);
		}
		if (query.hasOffset()) {
			results = results.skip(query.getOffset());
		}
		if (query.hasRows()) {
			results = results.limit(query.getRows());
		}
		return results;
	}

	<T> Stream<T> findAllUsingQuery(Class<T> type, Filter filter, Qualifier... qualifiers) {
		return findAllRecordsUsingQuery(type, filter, qualifiers)
				.map(keyRecord -> mapToEntity(keyRecord.key, type, keyRecord.record));
	}

	<T> Stream<KeyRecord> findAllRecordsUsingQuery(Class<T> type, Filter filter, Qualifier... qualifiers) {
		String setName = getSetName(type);

		KeyRecordIterator recIterator = this.queryEngine.select(
				this.namespace, setName, filter, qualifiers);

		return StreamUtils.createStreamFromIterator(recIterator)
				.onClose(() -> {
					try {
						recIterator.close();
					} catch (Exception e) {
						log.error("Caught exception while closing query", e);
					}
				});
	}

	<T> Stream<KeyRecord> findAllRecordsUsingQuery(Class<T> type, Query query) {
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(type, "Type must not be null!");

		Qualifier qualifier = query.getCriteria().getCriteriaObject();
		return findAllRecordsUsingQuery(type, null, qualifier);
	}
}
