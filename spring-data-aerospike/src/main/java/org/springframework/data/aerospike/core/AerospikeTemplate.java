/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.core;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.support.PropertyComparator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.AerospikeWriteData;
import org.springframework.data.aerospike.convert.CustomConversions;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentEntity;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.AerospikeSimpleTypes;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.repository.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.ConvertingPropertyAccessor;
import org.springframework.data.util.CloseableIterator;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.comparator.CompoundComparator;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.Value;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.ResultSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.helper.query.KeyRecordIterator;
import com.aerospike.helper.query.Qualifier;
import com.aerospike.helper.query.QueryEngine;

import lombok.extern.slf4j.Slf4j;


/**
 * Primary implementation of {@link AerospikeOperations}.
 * 
 * @author Oliver Gierke
 * @author Peter Milne
 */
@Slf4j
public class AerospikeTemplate implements AerospikeOperations {

	private final MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;
	private final AerospikeClient client;
	private final MappingAerospikeConverter converter;
	private final String namespace;
	private final QueryEngine queryEngine;
	private final AerospikeExceptionTranslator exceptionTranslator;

	
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
		Assert.notNull(client, "Aerospike client must not be null!");
		Assert.notNull(namespace, "Namespace cannot be null");
		Assert.hasLength(namespace, "Namespace cannot be empty");

		this.client = client;
		this.converter = converter;
		this.exceptionTranslator = exceptionTranslator;
		this.namespace = namespace;
		this.mappingContext = mappingContext;
		this.queryEngine = new QueryEngine(this.client);

		loggerSetup();
	}

	/**
	 * Instead use the other constructor.
	 */
	@Deprecated
	public AerospikeTemplate(AerospikeClient client, String namespace) {
		Assert.notNull(client, "Aerospike client must not be null!");
		Assert.notNull(namespace, "Namespace cannot be null");
		Assert.hasLength(namespace, "Namespace cannot be empty");

		CustomConversions customConversions = new CustomConversions(Collections.emptyList(), AerospikeSimpleTypes.HOLDER);
		AerospikeMappingContext asContext = new AerospikeMappingContext();
		asContext.setDefaultNameSpace(namespace);

		this.client = client;
		this.converter = new MappingAerospikeConverter(asContext, customConversions, new AerospikeTypeAliasAccessor());
		this.exceptionTranslator = new DefaultAerospikeExceptionTranslator();
		this.namespace = namespace;
		this.mappingContext = asContext;
		this.queryEngine = new QueryEngine(this.client);

		this.converter.afterPropertiesSet();

		loggerSetup();
	}

	private void loggerSetup() {
		final Logger log = LoggerFactory.getLogger("com.aerospike.client");
		com.aerospike.client.Log
				.setCallback(new com.aerospike.client.Log.Callback() {

					@Override
					public void log(com.aerospike.client.Log.Level level,
							String message) {
						switch (level) {
						case INFO:
							log.info("{}", message);
							break;
						case DEBUG:
							log.debug("{}", message);
							break;
						case ERROR:
							log.error("{}", message);
							break;
						case WARN:
							log.warn("{}", message);
							break;
						}

					}
				});
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
				throw new AerospikeException.InvalidNode();
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

		AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(document.getClass());

		if (entity.hasVersionProperty()) {
			doPersistWithCas(document, entity);
		} else {
			WritePolicyBuilder builder = WritePolicyBuilder.builder(this.client.writePolicyDefault)
					.sendKey(true)
					.recordExistsAction(RecordExistsAction.UPDATE);
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
	public boolean delete(Serializable id, Class<?> type) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(type, "Type must not be null!");
		try {
			AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
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
	public boolean exists(Serializable id, Class<?> type) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(type, "Type must not be null!");
		try {
			AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
			Key key = getKey(id, entity);

			Record record = this.client.operate(null, key, Operation.getHeader());
			return record != null;
		} catch (AerospikeException e) {
			DataAccessException translatedException = exceptionTranslator.translateExceptionIfPossible(e);
			throw translatedException == null ? e : translatedException;
		}
	}

	@Override
	public <T> List<T> findAll(final Class<T> type) {

		// TODO returning a list is dangerous because
		// the list is unbounded and could contain billions of elements
		// we need to find another solution
		final List<T> scanList = new ArrayList<T>();
		Iterable<T> results = findAllUsingQuery(type, null, (Qualifier[])null);
		Iterator<T> iterator = results.iterator();
		try {
			while (iterator.hasNext()) {
				scanList.add(iterator.next());
			}
		}
		finally {
			((EntityIterator<T>) iterator).close();
		}
		return scanList;
	}

	@Override
	public <T> T findById(Serializable id, Class<T> type) {
		Assert.notNull(id, "Id must not be null!");
		Assert.notNull(type, "Type must not be null!");
		try {
			AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
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

	/**
	 * Instead use findByIds
	 */
	@Deprecated
	@Override
	public 	<T> List<T> findByIDs(Iterable<? extends Serializable> ids, Class<T> type){
		Assert.notNull(ids, "List of ids must not be null!");
		Assert.notNull(type, "Type must not be null!");

		return findByIdsInternal(IterableConverter.toList(ids), type);
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
			AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(type);

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

		AerospikePersistentEntity<?> entity = mappingContext
				.getPersistentEntity(outputType);

		Statement statement = new Statement();
		if (filter != null)
			statement.setFilters(filter);
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
	public String getSetName(Class<?> entityClass) {
		AerospikePersistentEntity<?> entity = mappingContext
				.getPersistentEntity(entityClass);
		return entity.getSetName();
	}

	@Override
	public <T> Iterable<T> findAll(Sort sort, Class<T> type) {
		// TODO Auto-generated method stub
		return null;
	}

	@SuppressWarnings("unused")
	private static boolean typeCheck(Class<?> requiredType, Object candidate) {
		return candidate == null ? true
				: ClassUtils.isAssignable(requiredType, candidate.getClass());
	}

	public boolean exists(Query query, Class<?> entityClass) {
		if (query == null) {
			throw new InvalidDataAccessApiUsageException(
					"Query passed in to exist can't be null");
		}

		Iterator<?> iterator = (Iterator<?>) find(query, entityClass).iterator();

		return iterator.hasNext();
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
	public int count(Query query, Class<?> type) {
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(type, "Type must not be null!");
		int i = 0;
		Iterator iterator = (Iterator<?>) find(query, type).iterator();
		for (; iterator.hasNext(); ++i)
			iterator.next();
		return i;
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
	public <T> Iterable<T> find(Query query, Class<T> type) {
		Assert.notNull(query, "Query must not be null!");
		Assert.notNull(type, "Type must not be null!");
		Qualifier qualifier = query.getCritieria().getCriteriaObject();
		final Iterable<T> results = findAllUsingQuery(type, null, qualifier);
		List<?> returnedList = IterableConverter.toList(results);
		if(results!=null && query.getSort()!=null){
			Comparator comparator = aerospikePropertyComparator(query);
			Collections.sort(returnedList, comparator);
		}
		return (Iterable<T>) returnedList;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Comparator<?> aerospikePropertyComparator(Query query ) {

		if (query == null || query.getSort() == null) {
			return null;
		}

		CompoundComparator compoundComperator = new CompoundComparator();
		for (Order order : query.getSort()) {

			if (Direction.DESC.equals(order.getDirection())) {
				compoundComperator.addComparator(new PropertyComparator(order.getProperty(), true, false));
			}else {
				compoundComperator.addComparator(new PropertyComparator(order.getProperty(), true, true));
			}
		}

		return compoundComperator;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.data.aerospike.core.AerospikeOperations#
	 * getMappingContext()
	 */
	@Override
	public MappingContext<?, ?> getMappingContext() {
		return this.mappingContext;
	}

	public String getNamespace() {
		return namespace;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.keyvalue.core.KeyValueOperations#findInRange(
	 * int, int, org.springframework.data.domain.Sort, java.lang.Class)
	 */
	@Override
	public <T> Iterable<T> findInRange(int offset, int rows, Sort sort,
			Class<T> type) {
		Assert.notNull(type, "Type for count must not be null!");
		final long rowCount = rows;
		final AtomicLong count = new AtomicLong(0);
		final Iterable<T> results = findAllUsingQuery(type, null, (Qualifier[])null);
		final Iterator<T> iterator = results.iterator();
		/*
		 * skip over offset
		 */

		for (int skip = 0; skip < offset; skip++) {
			if (iterator.hasNext())
				iterator.next();
		}
		/*
		 * setup the iterable litimed by 'rows'
		 */

		Iterable<T> returnList = new Iterable<T>() {

			@Override
			public Iterator<T> iterator() {
				return new Iterator<T>() {

					@Override
					public boolean hasNext() {
						if (count.get() == rowCount) {
							((EntityIterator<T>) iterator).close();
							return false;
						}
						else {
							return iterator.hasNext();
						}
					}

					@Override
					public T next() {
						if (count.addAndGet(1) <= rowCount) {
							return iterator.next();
						}
						else {
							return null;
						}
					}

					@Override
					public void remove() {

					}
				};
			}
		};
		return (Iterable<T>) returnList;// TODO:create a sort
	}

	@Override
	public long count(Class<?> type) {
		Assert.notNull(type, "Type for count must not be null!");
		AerospikePersistentEntity<?> entity = mappingContext
				.getPersistentEntity(type);
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

	protected <T> Iterable<T> findAllUsingQuery(Class<T> type, Filter filter, Qualifier... qualifiers) {
		final Class<T> classType = type;
		Statement stmt = new Statement();
		stmt.setNamespace(this.namespace);
		stmt.setSetName(this.getSetName(type));
		Iterable<T> results = null;

		final KeyRecordIterator recIterator = this.queryEngine.select(
				this.namespace, this.getSetName(type), filter, qualifiers);

		results = new Iterable<T>() {

			@Override
			public Iterator<T> iterator() {
				return new EntityIterator<T>(classType, converter, recIterator);
			}

		};
		return results;
	}

	public class EntityIterator<T> implements CloseableIterator<T> {
		private KeyRecordIterator keyRecordIterator;
		private Class<T> type;
		
		public EntityIterator(Class<T> type, MappingAerospikeConverter converter, KeyRecordIterator keyRecordIterator) {
			this.type = type;
			this.keyRecordIterator = keyRecordIterator;
		}

		@Override
		public boolean hasNext() {
			return this.keyRecordIterator.hasNext();
		}

		@Override
		public T next() {
			KeyRecord keyRecord = this.keyRecordIterator.next();
			return mapToEntity(keyRecord.key, type, keyRecord.record);
		}

		@Override
		public void close() {
			try {
				keyRecordIterator.close();
			}
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		@Override
		public void remove() {

		}
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

	private <T> T mapToEntity(Key key, Class<T> type, Record record) {
		if(record == null) {
			return null;
		}
		AerospikeReadData data = AerospikeReadData.forRead(key, record);
		T readEntity = converter.read(type, data);

		AerospikePersistentEntity<?> entity = mappingContext.getPersistentEntity(type);
		if (entity.hasVersionProperty()) {
			final ConvertingPropertyAccessor accessor = getPropertyAccessor(entity, readEntity);
			accessor.setProperty(entity.getVersionProperty(), record.generation);
		}

		return readEntity;
	}

	private ConvertingPropertyAccessor getPropertyAccessor(AerospikePersistentEntity<?> entity, Object source) {
		PersistentPropertyAccessor accessor = entity.getPropertyAccessor(source);
		return new ConvertingPropertyAccessor(accessor, converter.getConversionService());
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

	private WritePolicy getCasAwareWritePolicy(AerospikeWriteData data, AerospikePersistentEntity<?> entity,
											   ConvertingPropertyAccessor accessor) {
		WritePolicyBuilder builder = WritePolicyBuilder.builder(this.client.writePolicyDefault)
				.sendKey(true)
				.generationPolicy(GenerationPolicy.EXPECT_GEN_EQUAL)
				.expiration(data.getExpiration());

		Integer version = accessor.getProperty(entity.getVersionProperty(), Integer.class);
		boolean existingDocument = version != null && version > 0L;
		if (existingDocument) {
			//Updating existing document with generation
			builder.recordExistsAction(RecordExistsAction.REPLACE_ONLY)
					.generation(version);
		} else {
			// create new document. if exists we should fail with optimistic locking
			builder.recordExistsAction(RecordExistsAction.CREATE_ONLY);
		}

		return builder.build();
	}

	private Key getKey(Object id, AerospikePersistentEntity<?> entity) {
		return new Key(this.namespace, entity.getSetName(), id.toString());
	}
}
