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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.core.AerospikeOperations;

import static org.assertj.core.api.Assertions.assertThat;

public class AerospikeCacheManagerIntegrationTests extends BaseBlockingIntegrationTests {

    private static final String KEY = "foo";
    private static final String KEY_THAT_MATCHES_CONDITION = "abcdef";
    private static final String VALUE = "bar";

    @Autowired
    IAerospikeClient client;
    @Autowired
    CachingComponent cachingComponent;
    @Autowired
    AerospikeOperations aerospikeOperations;
    @Autowired
    AerospikeCacheManager aerospikeCacheManager;

    @AfterEach
    public void tearDown() {
        cachingComponent.reset();
        client.delete(null, new Key(getNameSpace(), DEFAULT_SET_NAME, KEY));
        client.delete(null, new Key(getNameSpace(), DEFAULT_SET_NAME, KEY_THAT_MATCHES_CONDITION));
    }

    @Test
    public void shouldCache() {
        CachedObject response1 = cachingComponent.cacheableMethod(KEY);
        CachedObject response2 = cachingComponent.cacheableMethod(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldEvictCache() {
        CachedObject response1 = cachingComponent.cacheableMethod(KEY);
        cachingComponent.cacheEvictMethod(KEY);
        CachedObject response2 = cachingComponent.cacheableMethod(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
    }

    @Test
    public void shouldNotEvictCacheEvictingDifferentKey() {
        CachedObject response1 = cachingComponent.cacheableMethod(KEY);
        cachingComponent.cacheEvictMethod("not-the-relevant-key");
        CachedObject response2 = cachingComponent.cacheableMethod(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheUsingCachePut() {
        CachedObject response1 = cachingComponent.cachePutMethod(KEY);
        CachedObject response2 = cachingComponent.cacheableMethod(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheKeyMatchesCondition() {
        CachedObject response1 = cachingComponent.cacheableWithCondition(KEY_THAT_MATCHES_CONDITION);
        CachedObject response2 = cachingComponent.cacheableWithCondition(KEY_THAT_MATCHES_CONDITION);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldNotCacheKeyDoesNotMatchCondition() {
        CachedObject response1 = cachingComponent.cacheableWithCondition(KEY);
        CachedObject response2 = cachingComponent.cacheableWithCondition(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
    }

    @Test
    public void shouldCacheWithConfiguredTTL() throws InterruptedException {
        CachedObject response1 = cachingComponent.cacheableMethodWithTTL(KEY);
        CachedObject response2 = cachingComponent.cacheableMethodWithTTL(KEY);

        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);

        Thread.sleep(3000);

        CachedObject response3 = cachingComponent.cacheableMethodWithTTL(KEY);

        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(response3).isNotNull();
        assertThat(response3.getValue()).isEqualTo(VALUE);
    }

    @Test
    public void shouldCacheUsingAnotherCacheManager() {
        CachedObject response1 = cachingComponent.cacheableMethodWithAnotherCacheManager(KEY);
        CachedObject response2 = cachingComponent.cacheableMethodWithAnotherCacheManager(KEY);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldNotClearCacheClearingDifferentCache() throws InterruptedException {
        CachedObject response1 = cachingComponent.cacheableMethod(KEY);
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(1);
        aerospikeCacheManager.getCache("DIFFERENT-EXISTING-CACHE").clear();
        Thread.sleep(500);
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(1);
    }

    public static class CachingComponent {

        private int noOfCalls = 0;

        public void reset() {
            noOfCalls = 0;
        }

        @Cacheable("TEST")
        public CachedObject cacheableMethod(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable(value = "CACHE-WITH-TTL")
        public CachedObject cacheableMethodWithTTL(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable(value = "TEST", cacheManager = "anotherCacheManager")
        public CachedObject cacheableMethodWithAnotherCacheManager(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @CacheEvict("TEST")
        public void cacheEvictMethod(String param) {

        }

        @CachePut("TEST")
        public CachedObject cachePutMethod(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable(value = "TEST", condition = "#param.startsWith('abc')")
        public CachedObject cacheableWithCondition(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        public int getNoOfCalls() {
            return noOfCalls;
        }
    }

    public static class CachedObject {
        private String value;

        public CachedObject(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
