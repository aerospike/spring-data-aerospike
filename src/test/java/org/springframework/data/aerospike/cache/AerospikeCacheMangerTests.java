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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.transaction.TransactionAwareCacheDecorator;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 
 * @author Venil Noronha
 */
public class AerospikeCacheMangerTests extends BaseBlockingIntegrationTests {

	@Value("${embedded.aerospike.namespace}")
	public String namespace;

	@Autowired
	IAerospikeClient client;
	@Autowired
	MappingAerospikeConverter converter;

	@Test
	public void missingCache() {
		AerospikeCacheConfiguration aerospikeCacheConfiguration = new AerospikeCacheConfiguration(namespace, DEFAULT_SET_NAME);
		AerospikeCacheManager manager = new AerospikeCacheManager(client, converter, aerospikeCacheConfiguration);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("missing-cache");

		assertThat(cache).isNotNull();
	}

	@Test
	public void defaultCache() {
		AerospikeCacheConfiguration aerospikeCacheConfiguration = new AerospikeCacheConfiguration(namespace, DEFAULT_SET_NAME);
		Map<String, AerospikeCacheConfiguration> aerospikeCacheConfigurationMap = new HashMap<>();
		aerospikeCacheConfigurationMap.put("default-cache", aerospikeCacheConfiguration);
		AerospikeCacheManager manager = new AerospikeCacheManager(client, converter, aerospikeCacheConfiguration, aerospikeCacheConfigurationMap);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("default-cache");

		assertThat(cache).isNotNull().isInstanceOf(AerospikeCache.class);
	}

	@Test
	public void defaultCacheWithCustomizedSet() {
		Map<String, AerospikeCacheConfiguration> aerospikeCacheConfigurationMap = new HashMap<>();
		AerospikeCacheConfiguration aerospikeCacheConfiguration = new AerospikeCacheConfiguration(namespace, "custom-set");
		aerospikeCacheConfigurationMap.put("default-cache",aerospikeCacheConfiguration);
		AerospikeCacheManager manager = new AerospikeCacheManager(client, converter, aerospikeCacheConfiguration, aerospikeCacheConfigurationMap);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("default-cache");

		assertThat(cache).isNotNull().isInstanceOf(AerospikeCache.class);
	}

	@Test
	public void transactionAwareCache() {
		AerospikeCacheConfiguration aerospikeCacheConfiguration = new AerospikeCacheConfiguration(namespace, DEFAULT_SET_NAME);
		AerospikeCacheManager manager = new AerospikeCacheManager(client, converter, aerospikeCacheConfiguration);
		manager.setTransactionAware(true);
		manager.afterPropertiesSet();
		Cache cache = manager.getCache("transaction-aware-cache");

		assertThat(cache).isNotNull().isInstanceOf(TransactionAwareCacheDecorator.class);
	}
}
