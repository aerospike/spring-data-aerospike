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
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.interceptor.SimpleKey;
import org.springframework.data.aerospike.BaseBlockingIntegrationTests;
import org.springframework.data.aerospike.core.AerospikeOperations;
import org.springframework.data.aerospike.util.AwaitilityUtils;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.aerospike.util.AwaitilityUtils.awaitTenSecondsUntil;

public class AerospikeCacheManagerIntegrationTests extends BaseBlockingIntegrationTests {

    private static final String STRING_PARAM = "foo";
    private static final String STRING_PARAM_THAT_MATCHES_CONDITION = "abcdef";
    private static final long NUMERIC_PARAM = 100L;
    private static final Map<String, String> MAP_PARAM =
        Map.of("1", "val1", "2", "val2", "3", "val3", "4", "val4");
    private static final String VALUE = "bar";

    @Autowired
    IAerospikeClient client;
    @Autowired
    CachingComponent cachingComponent;
    @Autowired
    AerospikeOperations aerospikeOperations;
    @Autowired
    AerospikeCacheManager aerospikeCacheManager;

    @BeforeEach
    public void setup() {
        cachingComponent.reset();
        deleteRecords();
    }

    private void deleteRecords() {
        List<Integer> hashCodes = List.of(
            STRING_PARAM.hashCode(),
            STRING_PARAM_THAT_MATCHES_CONDITION.hashCode(),
            Long.hashCode(NUMERIC_PARAM),
            MAP_PARAM.hashCode(),
            new SimpleKey(STRING_PARAM, NUMERIC_PARAM, MAP_PARAM).hashCode());
        for (int hash : hashCodes) {
            client.delete(null, new Key(getNameSpace(), DEFAULT_SET_NAME, hash));
        }
        client.delete(null, new Key(getNameSpace(), DIFFERENT_SET_NAME, STRING_PARAM.hashCode()));
    }

    @Test
    public void shouldCache() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethod(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethod(STRING_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheWithNumericParam() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithNumericParam(NUMERIC_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethodWithNumericParam('d');

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheWithMapParam() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithMapParam(MAP_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethodWithMapParam(MAP_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheWithMultipleParams() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithMultipleParams(STRING_PARAM, NUMERIC_PARAM,
            MAP_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethodWithMultipleParams(STRING_PARAM, NUMERIC_PARAM,
            MAP_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheUsingDefaultSet() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        // default cache configuration is used for all cache names not pre-configured via AerospikeCacheManager
        CachedObject response1 = cachingComponent.cacheableMethodDefaultCache(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethod(STRING_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(1);
    }

    @Test
    public void shouldEvictCache() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethod(STRING_PARAM);
        cachingComponent.cacheEvictMethod(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethod(STRING_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
    }

    @Test
    public void shouldNotEvictCacheEvictingDifferentParam() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethod(STRING_PARAM);
        cachingComponent.cacheEvictMethod("not-the-relevant-param");
        CachedObject response2 = cachingComponent.cacheableMethod(STRING_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheUsingCachePut() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cachePutMethod(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethod(STRING_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);

        CachedObject response3 = cachingComponent.cachePutMethod(STRING_PARAM);
        assertThat(response3).isNotNull();
        assertThat(response3.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
    }

    @Test
    public void shouldCacheKeyMatchesCondition() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableWithCondition(STRING_PARAM_THAT_MATCHES_CONDITION);
        CachedObject response2 = cachingComponent.cacheableWithCondition(STRING_PARAM_THAT_MATCHES_CONDITION);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldNotCacheKeyDoesNotMatchCondition() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableWithCondition(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableWithCondition(STRING_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
    }

    @Test
    public void shouldCacheWithConfiguredTTL() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithTTL(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethodWithTTL(STRING_PARAM);

        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);

        awaitTenSecondsUntil(() -> {
            CachedObject response3 = cachingComponent.cacheableMethodWithTTL(STRING_PARAM);
            assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
            assertThat(response3).isNotNull();
            assertThat(response3.getValue()).isEqualTo(VALUE);
        });
    }

    @Test
    public void shouldCacheUsingAnotherCacheManager() {
        assertThat(aerospikeOperations.count(DIFFERENT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithAnotherCacheManager(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethodWithAnotherCacheManager(STRING_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldNotClearCacheClearingDifferentCache() {
        assertThat(aerospikeOperations.count(DIFFERENT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethod(STRING_PARAM);
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(1);
        aerospikeCacheManager.getCache(DIFFERENT_EXISTING_CACHE).clear();
        AwaitilityUtils.awaitTwoSecondsUntil(() -> {
            assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(1);
            assertThat(response1).isNotNull();
            assertThat(response1.getValue()).isEqualTo(VALUE);
        });
    }

    @Test
    public void shouldCacheUsingDifferentSet() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        assertThat(aerospikeOperations.count(DIFFERENT_SET_NAME)).isEqualTo(0);

        CachedObject response1 = cachingComponent.cacheableMethodDifferentExistingCache(STRING_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethodDifferentExistingCache(STRING_PARAM);
        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        AwaitilityUtils.awaitTwoSecondsUntil(() -> {
            assertThat(aerospikeOperations.count(DIFFERENT_SET_NAME)).isEqualTo(1);
            assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
            assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
        });


        CachedObject response3 = cachingComponent.cacheableMethod(STRING_PARAM);
        assertThat(response3).isNotNull();
        assertThat(response3.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(2);
        assertThat(aerospikeOperations.count(DIFFERENT_SET_NAME)).isEqualTo(1);
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(1);
    }

    public static class CachingComponent {

        private int noOfCalls = 0;

        public void reset() {
            noOfCalls = 0;
        }

        @Cacheable("TEST") // "TEST" is a cache name not pre-configured in AerospikeCacheManager, so goes to default set
        public CachedObject cacheableMethod(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable("TEST")
        public CachedObject cacheableMethodWithNumericParam(long param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable("TEST")
        public CachedObject cacheableMethodWithMapParam(Map<String, String> param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable("TEST")
        public CachedObject cacheableMethodWithMultipleParams(String param1, long param2, Map<String, String> param3) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable("TEST12345ABC") // Cache name not pre-configured in AerospikeCacheManager, so it goes to default set
        public CachedObject cacheableMethodDefaultCache(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable(DIFFERENT_EXISTING_CACHE)
        public CachedObject cacheableMethodDifferentExistingCache(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable(value = CACHE_WITH_TTL)
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

    @AllArgsConstructor
    public static class CachedObject {

        private final Object value;

        public Object getValue() {
            return value;
        }
    }
}
