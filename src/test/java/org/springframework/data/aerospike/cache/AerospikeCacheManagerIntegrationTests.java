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
import lombok.NoArgsConstructor;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.data.aerospike.util.AwaitilityUtils.awaitTenSecondsUntil;

@SuppressWarnings("NewObjectEquality")
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
    public void setup() throws NoSuchMethodException {
        cachingComponent.reset();
        deleteRecords();
    }

    private void deleteRecords() throws NoSuchMethodException {
        List<Object> params = List.of(
            STRING_PARAM,
            STRING_PARAM_THAT_MATCHES_CONDITION,
            NUMERIC_PARAM,
            MAP_PARAM,
            new SimpleKey(STRING_PARAM, NUMERIC_PARAM, MAP_PARAM),
            SimpleKey.EMPTY,
            new SimpleKey((Object) null),
            CachingComponent.class,
            CachingComponent.class.getMethod("cacheableMethodWithMethodNameKey")
        );
        for (Object param : params) {
            AerospikeCacheKey cacheKey = cacheKeyProcessor.serializeAndHash(param);
            client.delete(null, new Key(getNameSpace(), DEFAULT_SET_NAME, cacheKey.getValue()));
        }
        client.delete(null, new Key(getNameSpace(), DIFFERENT_SET_NAME,
            cacheKeyProcessor.serializeAndHash(STRING_PARAM).getValue()));
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
    @SuppressWarnings("StringEquality")
    public void shouldCacheInstances() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        String stringInstance1 = new String(STRING_PARAM.toCharArray());
        String stringInstance2 = new String(STRING_PARAM.toCharArray());
        // assert that variables are referencing different objects in memory
        assertThat(stringInstance1 != stringInstance2).isTrue();

        CachedObject response1 = cachingComponent.cacheableMethod(stringInstance1);
        CachedObject response2 = cachingComponent.cacheableMethod(stringInstance2);

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
    public void shouldCacheWithNthParam() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithNthParam(STRING_PARAM, NUMERIC_PARAM,
            MAP_PARAM);
        CachedObject response2 = cachingComponent.cacheableMethodWithNthParam(STRING_PARAM, NUMERIC_PARAM,
            MAP_PARAM);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheWithNoParams() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithNoParams();
        CachedObject response2 = cachingComponent.cacheableMethodWithNoParams();

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheWithNullParam() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethod(null);
        CachedObject response2 = cachingComponent.cacheableMethod(null);

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldNotCacheDifferentMethodsWithNoParamsByDefault_NegativeTest() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithNoParams();
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);

        // any two methods with no arguments will have the same cache key by default,
        // to change it set the necessary Cacheable annotation parameters (e.g. "key")
        assertThatThrownBy(() -> cachingComponent.anotherMethodWithNoParams())
            .isInstanceOf(ClassCastException.class)
            .hasMessageMatching(".+CachedObject cannot be cast to class .+AnotherCachedObject.*");
    }

    @Test
    public void shouldCacheWithTargetClassKey() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithTargetClassKey();
        CachedObject response2 = cachingComponent.cacheableMethodWithTargetClassKey();

        assertThat(response1).isNotNull();
        assertThat(response1.getValue()).isEqualTo(VALUE);
        assertThat(response2).isNotNull();
        assertThat(response2.getValue()).isEqualTo(VALUE);
        assertThat(cachingComponent.getNoOfCalls()).isEqualTo(1);
    }

    @Test
    public void shouldCacheWithMethodNameKey() {
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
        CachedObject response1 = cachingComponent.cacheableMethodWithMethodNameKey();
        CachedObject response2 = cachingComponent.cacheableMethodWithMethodNameKey();

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
    public void testCacheableMethodSync() throws InterruptedException {
        assertThat(cachingComponent.getNoOfCalls() == 0).isTrue();

        // Creating two threads that will call cacheableMethod concurrently
        Thread thread1 = new Thread(() -> {
            CachedObject response = cachingComponent.cacheableMethodSynchronized(STRING_PARAM);
            assertThat(response).isNotNull();
            assertThat(response.getValue()).isEqualTo(VALUE);
        });

        Thread thread2 = new Thread(() -> {
            CachedObject response = cachingComponent.cacheableMethodSynchronized(STRING_PARAM);
            assertThat(response).isNotNull();
            assertThat(response.getValue()).isEqualTo(VALUE);
        });

        // Starting both threads
        thread1.start();
        thread2.start();

        // Waiting for both threads to complete
        thread1.join();
        thread2.join();

        // Expecting method to be called only once due to synchronization
        assertThat(cachingComponent.getNoOfCalls() == 1).isTrue();
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
        assertThat(aerospikeOperations.count(DEFAULT_SET_NAME)).isEqualTo(0);
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

        @Cacheable(value = "TEST", key = "#root.args[1]")
        public CachedObject cacheableMethodWithNthParam(String param1, long param2, Map<String, String> param3) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable("TEST")
        public CachedObject cacheableMethodWithNoParams() {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable("TEST")
        public AnotherCachedObject anotherMethodWithNoParams() {
            noOfCalls++;
            return new AnotherCachedObject(NUMERIC_PARAM);
        }

        @Cacheable(value = "TEST", key = "#root.targetClass")
        public CachedObject cacheableMethodWithTargetClassKey() {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable(value = "TEST", key = "#root.method")
        public CachedObject cacheableMethodWithMethodNameKey() {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @Cacheable(value = "TEST", sync = true)
        public CachedObject cacheableMethodSynchronized(String param) {
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

    @NoArgsConstructor
    @AllArgsConstructor
    public static class CachedObject {

        private Object value;

        public Object getValue() {
            return value;
        }
    }

    @NoArgsConstructor
    @AllArgsConstructor
    public static class AnotherCachedObject {

        private long number;

        public long getNumber() {
            return number;
        }
    }
}
