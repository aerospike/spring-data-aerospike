package org.springframework.data.aerospike.cache;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import org.junit.After;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.aerospike.BaseIntegrationTests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class AerospikeCacheManagerIntegrationTests extends BaseIntegrationTests {

    private static final String KEY = "foo";
    private static final String VALUE = "bar";

    @Autowired
    AerospikeClient client;
    @Autowired
    CachingComponent cachingComponent;

    @After
    public void tearDown() throws Exception {
        cachingComponent.reset();
        client.delete(null, new Key(getNameSpace(), AerospikeCacheManager.DEFAULT_SET_NAME, KEY));
    }

    @Test
    public void shouldCache() {
        CachedObject response1 = cachingComponent.cachingMethod(KEY);
        CachedObject response2 = cachingComponent.cachingMethod(KEY);
        assertNotNull("Component returned null", response1);
        assertEquals("Response didn't match", VALUE, response1.getValue());
        assertNotNull("Component returned null", response2);
        assertEquals("Response didn't match", VALUE, response2.getValue());
        assertEquals("Component didn't cache result", 1, cachingComponent.getNoOfCalls());
    }

    @Test
    public void shouldEvictCache() {
        CachedObject response1 = cachingComponent.cachingMethod(KEY);
        cachingComponent.cacheEvictingMethod(KEY);
        CachedObject response2 = cachingComponent.cachingMethod(KEY);
        assertNotNull("Component returned null", response1);
        assertEquals("Response didn't match", VALUE, response1.getValue());
        assertNotNull("Component returned null", response2);
        assertEquals("Response didn't match", VALUE, response2.getValue());
        assertEquals("Component didn't evict cached entry", 2, cachingComponent.getNoOfCalls());
    }

    public static class CachingComponent {

        private int noOfCalls = 0;

        public void reset() {
            noOfCalls = 0;
        }

        @Cacheable("TEST")
        public CachedObject cachingMethod(String param) {
            noOfCalls++;
            return new CachedObject(VALUE);
        }

        @CacheEvict("TEST")
        public void cacheEvictingMethod(String param) {

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
