/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.aerospike.cache;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.data.aerospike.convert.AerospikeConverter;
import org.springframework.data.aerospike.convert.AerospikeReadData;
import org.springframework.data.aerospike.convert.AerospikeWriteData;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * A Cache {@link org.springframework.cache.Cache} implementation backed by Aerospike database as store.
 * Create and configure Aerospike cache instances via {@link AerospikeCacheManager}.
 *
 * @author Venil Noronha
 */
public class AerospikeCache implements Cache {

	private static final String VALUE = "value";

	private final String name;
	private final IAerospikeClient client;
	private final AerospikeConverter aerospikeConverter;
	private final AerospikeCacheConfiguration cacheConfiguration;
	private final WritePolicy createOnly;
	private final WritePolicy writePolicyForPut;

	public AerospikeCache(String name,
						  IAerospikeClient client,
						  AerospikeConverter aerospikeConverter,
						  AerospikeCacheConfiguration cacheConfiguration) {
		this.name = name;
		this.client = client;
		this.aerospikeConverter = aerospikeConverter;
		this.cacheConfiguration = cacheConfiguration;
		this.createOnly = new WritePolicy(client.getWritePolicyDefault());
		this.createOnly.recordExistsAction = RecordExistsAction.CREATE_ONLY;
		this.createOnly.expiration = cacheConfiguration.getExpirationInSeconds();
		this.writePolicyForPut = new WritePolicy(client.getWritePolicyDefault());
		this.writePolicyForPut.expiration = cacheConfiguration.getExpirationInSeconds();
	}

	/**
	 * Clears the cache by truncating the configured cache's set (in the configured namespace).
	 */
	@Override
	public void clear() {
		client.truncate(null, cacheConfiguration.getNamespace(), cacheConfiguration.getSet(), null);
	}

	/**
	 * Deletes the key from Aerospike database.
	 * @param key The key to delete.
	 */
	@Override
	public void evict(Object key) {
		client.delete(null, getKey(key));
	}

	/**
	 * Get cache's name.
	 * @return The cache's name.
	 */
	@Override
	public String getName() {
		return name;
	}

	/**
	 * Get the underlying native cache provider - the Aerospike client.
	 * @return The aerospike client.
	 */
	@Override
	public Object getNativeCache() {
		return client;
	}

	/**
	 * Return the value (bins) from the Aerospike database to which this cache maps the specified key, obtaining that value from valueLoader if necessary.
	 * This method provides a simple substitute for the conventional "if cached, return; otherwise create, cache and return" pattern.
	 * @param key The key whose associated value is to be returned.
	 * @param valueLoader The value loader that might contain the value (bins).
	 * @return The value (bins) to which this cache maps the specified key.
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T> T get(Object key, Callable<T> valueLoader) {
		T value = (T) client.get(null, getKey(key)).getValue(VALUE);
		if (Objects.isNull(value)) {
			try {
				value = valueLoader.call();
				if (Objects.nonNull(value)) {
					put(key, value);
				}
			} catch (Throwable e) {
				throw new Cache.ValueRetrievalException(key, valueLoader, e);
			}
		}
		return value;
	}

	/**
	 * Return the value (bins) from the Aerospike database to which this cache maps the specified key.
	 * Generically specifying a type that return value will be cast to.
	 * @param key The key whose associated value (bins) is to be returned.
	 * @param type The required type of the returned value (may be null to bypass a type check; in case of a null value found in the cache, the specified type is irrelevant).
	 * @return The value (bins) to which this cache maps the specified key (which may be null itself), or also null if the cache contains no mapping for this key.
	 */
	@Override
	public <T> T get(Object key, Class<T> type) {
		Key dbKey = getKey(key);
		Record record =  client.get(null, dbKey);
		if (record != null) {
			AerospikeReadData data = AerospikeReadData.forRead(dbKey, record);
			return aerospikeConverter.read(type, data);
		}
		return null;
	}

	/**
	 * Returns the value (bins) from the Aerospike database to which this cache maps the specified key.
	 * Returns null if the cache contains no mapping for this key; otherwise, the cached value (which may be null itself) will be returned in a Cache.ValueWrapper.
	 * @param key The key whose associated value (bins) is to be returned.
	 * @return The value (bins) to which this cache maps the specified key, contained within a Cache.ValueWrapper which may also hold a cached null value.
	 * 	A straight null being returned means that the cache contains no mapping for this key.
	 */
	@Override
	public ValueWrapper get(Object key) {
		Object value = get(key, Object.class);
		return (value != null ? new SimpleValueWrapper(value) : null);
	}

	/**
	 * Write the key-value pair to Aerospike database.
	 * @param key The key to write.
	 * @param value The value to write.
	 */
	@Override
	public void put(Object key, Object value) {
		serializeAndPut(writePolicyForPut, key, value);
	}

	/**
	 * Write the key-value pair to Aerospike database if the key doesn't already exists.
	 * @param key The key to write.
	 * @param value The value (bins) to write.
	 * @return In case the key already exists return the existing value, else return null.
	 */
	@Override
	public ValueWrapper putIfAbsent(Object key, Object value) {
		ValueWrapper valueWrapper = get(key);
		// Key already exists, return the existing value
		if (valueWrapper != null) {
			return valueWrapper;
		}
		// Key doesn't exists, write the new given key-value to Aerospike database and return null
		serializeAndPut(createOnly, key, value);
		return null;
	}

	private Key getKey(Object key){
		return new Key(cacheConfiguration.getNamespace(), cacheConfiguration.getSet(), key.toString());
	}

	private void serializeAndPut(WritePolicy writePolicy, Object key, Object value) {
		AerospikeWriteData data = AerospikeWriteData.forWrite();
		aerospikeConverter.write(value, data);
		client.put(writePolicy, getKey(key), data.getBinsAsArray());
	}
}
