package org.springframework.data.aerospike.examples.blocking.caching;

import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.aerospike.examples.blocking.caching.entity.CachedMovie;

import static org.springframework.data.aerospike.examples.support.ExampleAssertions.require;

// Demonstrates Spring Cache annotations backed by Aerospike.
public class BlockingCachingExample {

    private final BlockingCachingMovieService service;
    private final CacheManager cacheManager;

    public BlockingCachingExample(BlockingCachingMovieService service, CacheManager cacheManager) {
        this.service = service;
        this.cacheManager = cacheManager;
    }

    public void run() {
        useCacheableMethod();
        updateCachedValue();
        evictCachedValue();

        System.out.println("Ran Aerospike-backed Spring cache annotations");
    }

    private void useCacheableMethod() {
        // tag::caching-cacheable-usage[]
        // Spring calls this service through a proxy, so @Cacheable can store the first result.
        CachedMovie first = service.findMovie("cache-movie-1");
        // The second call has the same cache key and is served from Aerospike-backed cache storage.
        CachedMovie cached = service.findMovie("cache-movie-1");
        // end::caching-cacheable-usage[]

        require(first.getTitle().equals(cached.getTitle()), "Expected cached movie title to match");
        require(service.loads() == 1, "Expected the second call to be served from cache");
    }

    private void updateCachedValue() {
        // tag::caching-cache-put-usage[]
        // @CachePut updates the cache entry while still running the service method body.
        service.updateMovie(new CachedMovie("cache-movie-1", "Sneakers Updated"));
        // The next lookup reads the replacement value from the cache without another load.
        CachedMovie updated = service.findMovie("cache-movie-1");
        // end::caching-cache-put-usage[]

        require("Sneakers Updated".equals(updated.getTitle()), "Expected cache put to replace cached value");
        require(service.loads() == 1, "Expected updated value to be loaded from cache");
    }

    private void evictCachedValue() {
        // tag::caching-cache-evict-usage[]
        // @CacheEvict removes the cached value so the following @Cacheable call reloads it.
        service.evictMovie("cache-movie-1");
        CachedMovie reloaded = service.findMovie("cache-movie-1");
        // end::caching-cache-evict-usage[]

        require("Sneakers".equals(reloaded.getTitle()), "Expected value to reload after eviction");
        require(service.loads() == 2, "Expected one reload after eviction");
        require(cache("movies").get("cache-movie-1", CachedMovie.class) != null, "Expected cache entry to exist");
    }

    private Cache cache(String name) {
        Cache cache = cacheManager.getCache(name);
        require(cache != null, "Expected cache to be available: " + name);
        return cache;
    }
}
