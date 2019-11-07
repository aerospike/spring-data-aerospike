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

import com.aerospike.client.*;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.*;
import com.aerospike.client.task.IndexTask;
import com.aerospike.helper.query.Qualifier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


/**
 * Primary implementation of {@link AerospikeOperations}.
 * 
 * @author Oliver Gierke
 * @author Peter Milne
 */
@Slf4j
public class AerospikeTemplate extends BaseAerospikeTemplate implements AerospikeOperations {

	/**
	 * Creates a new {@link AerospikeTemplate} for the given
	 * {@link AerospikeClient}.
	 * 
	 * @param converter
	 * @param mappingContext
	 * @param exceptionTranslator
	 * @param client must not be {@literal null}.
	 * @param namespace must not be {@literal null} or empty.
	 */
	public AerospikeTemplate(AerospikeClient client, String namespace, MappingAerospikeConverter converter,
							 AerospikeMappingContext mappingContext,
							 AerospikeExceptionTranslator exceptionTranslator) {
        super(client, namespace, converter, mappingContext, exceptionTranslator);
	}

	/**
	 * Instead use the other constructor.
	 */
	@Deprecated
	public AerospikeTemplate(AerospikeClient client, String namespace) {
	    super(client, namespace);
	}

	@Override
	public <T> void createIndex(Class<T> domainType, String indexName,
								String binName, IndexType indexType) {
		try {
			String setName = getSetName(domainType);
			IndexTask task = client.createIndex(null, this.namespace,
					setName, indexName, binName, indexType);
			if (task != null) {
				task.waitTillComplete();
			}
			queryEngine.refreshIndexes();
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@Override
	public <T> void deleteIndex(Class<T> domainType, String indexName) {
		try {
			String setName = getSetName(domainType);
			IndexTask task = client.dropIndex(null, this.namespace, setName, indexName);
			if (task != null) {
				task.waitTillComplete();
			}
			queryEngine.refreshIndexes();
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@Override
	public boolean indexExists(String indexName) {
		//TODO: should be moved to aerospike-client
		try {
			Node[] nodes = client.getNodes();
			if (nodes.length == 0) {
				throw new AerospikeException(ResultCode.SERVER_NOT_AVAILABLE, "Command failed because cluster is empty.");
			}
			Node node = nodes[0];
			String response = Info.request(node, "sindex/" + namespace + '/' + indexName);
			return !response.startsWith("FAIL:201");
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@Override
	public void save(Object document) {
		Assert.notNull(document, "Object to insert must not be null!");

		AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(document.getClass());

		if (entity.hasVersionProperty()) {
			doPersistWithCas(document, entity);
		} else {
			WritePolicyBuilder builder = WritePolicyBuilder.builder(this.client.writePolicyDefault)
					.sendKey(true)
					.recordExistsAction(RecordExistsAction.REPLACE);
			doPersist(document, builder);
		}
	}

	@Override
	public void persist(Object document, WritePolicy policy) {
		Assert.notNull(document, "Document must not be null!");
		Assert.notNull(policy, "Policy must not be null!");

		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(document, data);

			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();

			client.put(policy, key, bins);
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	public <T> void insertAll(Collection<? extends T> documents) {
		Assert.notNull(documents, "Documents must not be null!");
		documents.stream().filter(Objects::nonNull).forEach(this::insert);
	}

	@Override
	public void insert(Object document) {
		Assert.notNull(document, "Document must not be null!");

		WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(this.client.writePolicyDefault)
				.sendKey(true)
				.recordExistsAction(RecordExistsAction.CREATE_ONLY);

		doPersist(document, writePolicyBuilder);
	}

	@Override
	public void update(Object document) {
		Assert.notNull(document, "Document must not be null!");

		WritePolicyBuilder writePolicyBuilder = WritePolicyBuilder.builder(this.client.writePolicyDefault)
				.sendKey(true)
				.recordExistsAction(RecordExistsAction.UPDATE_ONLY);

		doPersist(document, writePolicyBuilder);
	}

	@Override
	public void delete(Class<?> type) {
		try {
			String set = getSetName(type);
			client.truncate(null, getNamespace(), set, null);
		}
		catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator
					.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@Override
	public boolean delete(Object id, Class<?> type) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(type, "Type must not be null!");
		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
			Key key = getKey(id, entity);

			return this.client.delete(null, key);
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@Override
	public boolean delete(Object objectToDelete) {
		Assert.notNull(objectToDelete, "Object to delete must not be null!");
		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(objectToDelete, data);

			return this.client.delete(null, data.getKey());
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@Override
	public boolean exists(Object id, Class<?> type) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(type, "Type must not be null!");
		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
			Key key = getKey(id, entity);

			Record record = this.client.operate(null, key, Operation.getHeader());
			return record != null;
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@Override
	public <T> Stream<T> findAll(final Class<T> type) {
		return findAllUsingQuery(type, null, (Qualifier[])null);
	}

	@Override
	public <T> T findById(Object id, Class<T> type) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(type, "Type must not be null!");
		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);
			Key key = getKey(id, entity);

			Record record;
			if (entity.isTouchOnRead()) {
				Assert.state(!entity.hasExpirationProperty(), "Touch on read is not supported for expiration property");
				record = getAndTouch(key, entity.getExpiration());
			} else {
				record = this.client.get(null, key);
			}

			return mapToEntity(key, type, record);
		}
		catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	private Record getAndTouch(Key key, int expiration) {
		WritePolicy writePolicy = new WritePolicy(client.writePolicyDefault);
		writePolicy.expiration = expiration;

		if (this.client.exists(null, key)) {
			return this.client.operate(writePolicy, key, Operation.touch(), Operation.get());
		}

		return null;
	}

	@Override
	public <T> List<T> findByIds(Iterable<?> ids, Class<T> type) {
		Assert.notNull(ids, "List of ids must not be null!");
		Assert.notNull(type, "Type must not be null!");

		return findByIdsInternal(IterableConverter.toList(ids), type);
	}

	private <T> List<T> findByIdsInternal(Collection<?> ids, Class<T> type) {
		if (ids.isEmpty()) {
			return Collections.emptyList();
		}

		try {
			AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);

			Key[] keys = ids.stream()
					.map(id -> getKey(id, entity))
					.toArray(Key[]::new);

			Record[] records = client.get(null, keys);

			return IntStream.range(0, keys.length)
					.filter(index -> records[index] != null)
					.mapToObj(index -> mapToEntity(keys[index], type, records[index]))
					.collect(Collectors.toList());
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> Iterable<T> aggregate(Filter filter, Class<T> outputType,
			String module, String function, List<Value> arguments) {
		Assert.notNull(outputType, "Output type must not be null!");

		AerospikePersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(outputType);

		Statement statement = new Statement();
		if (filter != null)
			statement.setFilter(filter);
		statement.setSetName(entity.getSetName());
		statement.setNamespace(this.namespace);
		ResultSet resultSet = null;
		if (arguments != null && arguments.size() > 0)
			resultSet = this.client.queryAggregate(null, statement, module,
					function, arguments.toArray(new Value[0]));
		else
			resultSet = this.client.queryAggregate(null, statement);
		return (Iterable<T>) resultSet;
	}

	@Override
	public <T> Iterable<T> findAll(Sort sort, Class<T> type) {
		throw new UnsupportedOperationException("not implemented");
	}

	@SuppressWarnings("unused")
	private static boolean typeCheck(Class<?> requiredType, Object candidate) {
		return candidate == null || ClassUtils.isAssignable(requiredType, candidate.getClass());
	}

	public boolean exists(Query query, Class<?> entityClass) {
		Assert.notNull(query, "Query passed in to exist can't be null");
		return find(query, entityClass).findAny().isPresent();
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.data.aerospike.core.AerospikeOperations#execute(java.util.function.Supplier)
	 */
	@Override
	public <T> T execute(Supplier<T> supplier) {
		Assert.notNull(supplier, "Callback must not be null!");
		try {
			return supplier.get();
		} catch (RuntimeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see
	 * org.springframework.data.aerospike.core.AerospikeOperations#count(org.
	 * springframework.data.aerospike.repository.query.Query, java.lang.Class)
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public long count(Query query, Class<?> type) {
		Stream<KeyRecord> results = findAllRecordsUsingQuery(type, query);
		return results.count();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.aerospike.core.AerospikeOperations#find(org.
	 * springframework.data.aerospike.repository.query.Query, java.lang.Class)
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public <T> Stream<T> find(Query query, Class<T> type) {
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(type, "Type must not be null!");
		return findAllUsingQuery(type, query);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueOperations#findInRange(
	 * int, int, org.springframework.data.domain.Sort, java.lang.Class)
	 */
	@Override
	public <T> Stream<T> findInRange(long offset, long limit, Sort sort,
									 Class<T> type) {
		Assert.notNull(type, "Type for count must not be null!");
		Stream<T> results = findAllUsingQuery(type, null, (Qualifier[])null);
		//TODO:create a sort
		return results.skip(offset).limit(limit);
	}

	@Override
	public long count(Class<?> type) {
		Assert.notNull(type, "Type for count must not be null!");
		AerospikePersistentEntity<?> entity = mappingContext
				.getRequiredPersistentEntity(type);
		return count(type, entity.getSetName());
	}

	@Override
	public AerospikeClient getAerospikeClient() {
		return client;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueOperations#count(java.lang
	 * .Class)
	 */
	@Override
	public long count(Class<?> type, String setName) {
		Assert.notNull(type, "Type for count must not be null!");
		Node[] nodes = client.getNodes();
		int replicationCount = 2;
		int nodeCount = nodes.length;
		int n_objects = 0;
		for (Node node : nodes) {
			String infoString = Info.request(node,
					"sets/" + this.namespace + "/" + setName);
			String n_objectsString = infoString.substring(
					infoString.indexOf("=") + 1, infoString.indexOf(":"));
			n_objects = Integer.parseInt(n_objectsString);
		}

		return (nodeCount > 1) ? n_objects / replicationCount : n_objects;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T prepend(T objectToPrependTo, String fieldName, String value) {
		Assert.notNull(objectToPrependTo,
				"Object to prepend to must not be null!");
		try {

			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(objectToPrependTo, data);
			Record record = this.client.operate(null, data.getKey(),
					Operation.prepend(new Bin(fieldName, value)),
					Operation.get(fieldName));

			return mapToEntity(data.getKey(), (Class<T>) objectToPrependTo.getClass(), record);
		}
		catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator
					.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T prepend(T objectToPrependTo, Map<String, String> values) {
		Assert.notNull(objectToPrependTo,
				"Object to prepend to must not be null!");
		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(objectToPrependTo, data);
			Operation[] ops = new Operation[values.size() + 1];
			int x = 0;
			for (Map.Entry<String, String> entry : values.entrySet()) {
				Bin newBin = new Bin(entry.getKey(), entry.getValue());
				ops[x] = Operation.prepend(newBin);
				x++;
			}
			ops[x] = Operation.get();
			Record record = this.client.operate(null, data.getKey(), ops);

			return mapToEntity(data.getKey(), (Class<T>) objectToPrependTo.getClass(), record);
		}
		catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator
					.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T append(T objectToAppendTo, Map<String, String> values) {
		Assert.notNull(objectToAppendTo,
				"Object to append to must not be null!");
		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(objectToAppendTo, data);
			Operation[] ops = new Operation[values.size() + 1];
			int x = 0;
			for (Map.Entry<String, String> entry : values.entrySet()) {
				Bin newBin = new Bin(entry.getKey(), entry.getValue());
				ops[x] = Operation.append(newBin);
				x++;
			}
			ops[x] = Operation.get();
			Record record = this.client.operate(null, data.getKey(), ops);

			return mapToEntity(data.getKey(), (Class<T>) objectToAppendTo.getClass(), record);
		}
		catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator
					.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T append(T objectToAppendTo, String binName, String value) {
		Assert.notNull(objectToAppendTo,
				"Object to append to must not be null!");
		try {

			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(objectToAppendTo, data);
			Record record = this.client.operate(null, data.getKey(),
					Operation.append(new Bin(binName, value)),
					Operation.get(binName));

			return mapToEntity(data.getKey(), (Class<T>) objectToAppendTo.getClass(), record);
		}
		catch (AerospikeException o_O) {
			DataAccessException translatedException = exceptionTranslator
					.translateExceptionIfPossible(o_O);
			throw translatedException == null ? o_O : translatedException;
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T add(T objectToAddTo, Map<String, Long> values) {
		Assert.notNull(objectToAddTo, "Object to add to must not be null!");
		Assert.notNull(values, "Values must not be null!");
		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(objectToAddTo, data);
			Operation[] operations = new Operation[values.size() + 1];
			int x = 0;
			for (Map.Entry<String, Long> entry : values.entrySet()) {
				Bin newBin = new Bin(entry.getKey(), entry.getValue());
				operations[x] = Operation.add(newBin);
				x++;
			}
			operations[x] = Operation.get();

			WritePolicy writePolicy = new WritePolicy(this.client.writePolicyDefault);
			writePolicy.expiration = data.getExpiration();

			Record record = this.client.operate(writePolicy, data.getKey(),
					operations);

			return mapToEntity(data.getKey(), (Class<T>) objectToAddTo.getClass(), record);
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@SuppressWarnings("unchecked")
	public <T> T add(T objectToAddTo, String binName, long value) {
		Assert.notNull(objectToAddTo, "Object to add to must not be null!");
		Assert.notNull(binName, "Bin name must not be null!");
		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(objectToAddTo, data);

			WritePolicy writePolicy = new WritePolicy(this.client.writePolicyDefault);
			writePolicy.expiration = data.getExpiration();

			Record record = this.client.operate(writePolicy, data.getKey(),
					Operation.add(new Bin(binName, value)), Operation.get());

			return mapToEntity(data.getKey(), (Class<T>) objectToAddTo.getClass(), record);
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	private void doPersist(Object document, WritePolicyBuilder policyBuilder) {
		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(document, data);

			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();
			WritePolicy policy = policyBuilder.expiration(data.getExpiration())
					.build();

			client.put(policy, key, bins);
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	private void doPersistWithCas(Object document, AerospikePersistentEntity<?> entity) {
		try {
			AerospikeWriteData data = AerospikeWriteData.forWrite();
			converter.write(document, data);

			Key key = data.getKey();
			Bin[] bins = data.getBinsAsArray();

			ConvertingPropertyAccessor accessor = getPropertyAccessor(entity, document);
			WritePolicy policy = getCasAwareWritePolicy(data, entity, accessor);

			Operation[] operations = OperationUtils.operations(bins, Operation::put, Operation.getHeader());

			Record newRecord = client.operate(policy, key, operations);
			accessor.setProperty(entity.getVersionProperty(), newRecord.generation);
		} catch (AerospikeException e) {
			int code = e.getResultCode();
			if (code == ResultCode.KEY_EXISTS_ERROR || code == ResultCode.GENERATION_ERROR) {
				throw new OptimisticLockingFailureException("Save document with version value failed", e);
			}

			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}
}
