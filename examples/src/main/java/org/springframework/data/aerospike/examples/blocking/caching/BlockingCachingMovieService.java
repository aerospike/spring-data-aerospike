package org.springframework.data.aerospike.examples.blocking.caching;

import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.aerospike.examples.blocking.caching.entity.CachedMovie;

public class BlockingCachingMovieService {

    private int loads;

    // tag::caching-cacheable[]
    @Cacheable(cacheNames = "movies", key = "#id")
    public CachedMovie findMovie(String id) {
        loads++;
        return new CachedMovie(id, "Sneakers");
    }
    // end::caching-cacheable[]

    // tag::caching-cache-put[]
    @CachePut(cacheNames = "movies", key = "#movie.id")
    public CachedMovie updateMovie(CachedMovie movie) {
        return movie;
    }
    // end::caching-cache-put[]

    // tag::caching-cache-evict[]
    @CacheEvict(cacheNames = "movies", key = "#id")
    public void evictMovie(String id) {
    }
    // end::caching-cache-evict[]

    int loads() {
        return loads;
    }
}
