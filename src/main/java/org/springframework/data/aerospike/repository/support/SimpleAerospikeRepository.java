/*
 * Copyright 2012-2018 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.repository.support;

import com.aerospike.client.query.IndexType;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.keyvalue.core.IterableConverter;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.util.Assert;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SimpleAerospikeRepository<T, ID> implements AerospikeRepository<T, ID>, CrudRepository<T, ID> {

	private final AerospikeOperations operations;
	private final EntityInformation<T, ID> entityInformation;
	
	public SimpleAerospikeRepository(EntityInformation<T, ID> metadata,
			AerospikeOperations operations) {
		this.entityInformation = metadata;
		this.operations = operations;
	}

	@Override
	public Optional<T> findById(ID id) {
		return Optional.ofNullable(operations.findById(id, entityInformation.getJavaType()));
	}

	@Override
	public <S extends T> S save(S entity) {
		Assert.notNull(entity, "Cannot save NULL entity");

		operations.save(entity);
		return entity;
	}

	public <S extends T> List<S> saveAll(Iterable<S> entities) {
		Assert.notNull(entities, "The given Iterable of entities not be null!");

		List<S> result = IterableConverter.toList(entities);
		for (S entity : result) {
			save(entity);
		}

		return result;
	}
	
	@Override
	public void delete(T entity) {
		operations.delete(entity);
	}

	@Override
	public void deleteAllById(Iterable<? extends ID> iterable) {
		Assert.notNull(iterable, "The given Iterable must not be null!");
		iterable.forEach(this::deleteById);
	}

	@Override
	public Iterable<T> findAll(Sort sort) {
		Stream<T> findResults = operations.findAll(sort, 0, 0, entityInformation.getJavaType());
		return findResults::iterator;
	}

	@Override
	public Page<T> findAll(Pageable pageable) {
		if (pageable == null) {
			List<T> result = findAll();
			return new PageImpl<>(result, null, result.size());
		}

		Class<T> type = entityInformation.getJavaType();
		String setName = operations.getSetName(type);

		Stream<T> content = operations.findInRange(pageable.getOffset(), pageable.getPageSize(), pageable.getSort(), type);
		long totalCount = operations.count(setName);

		return new PageImpl<>(content.collect(Collectors.toList()), pageable, totalCount);
	}

	@Override
	public boolean existsById(ID id) {
		return operations.exists(id, entityInformation.getJavaType());
	}

	@Override
	public List<T> findAll() {
		return operations.findAll(entityInformation.getJavaType()).collect(Collectors.toList());
	}

	@Override
	public Iterable<T> findAllById(Iterable<ID> ids) {
		return operations.findByIds(ids, entityInformation.getJavaType());
	}

	@Override
	public long count() {
		return operations.count(entityInformation.getJavaType());
	}

	@Override
	public void deleteById(ID id) {
		Assert.notNull(id, "The given id must not be null!");
		operations.delete(id, entityInformation.getJavaType());
	}

	@Override
	public void deleteAll(Iterable<? extends T> entities) {
		for (T entity : entities) {
			delete(entity);
		}
	}

	@Override
	public void deleteAll() {
		operations.delete(entityInformation.getJavaType());
	}

	@Override
	public <E> void createIndex(Class<E> domainType, String indexName, String binName, IndexType indexType) {
		operations.createIndex(domainType, indexName, binName, indexType);
	}

	@Override
	public <E> void deleteIndex(Class<E> domainType, String indexName) {
		operations.deleteIndex(domainType, indexName);
	}

	@Override
	public boolean indexExists(String indexName) {
		return operations.indexExists(indexName);
	}
}
