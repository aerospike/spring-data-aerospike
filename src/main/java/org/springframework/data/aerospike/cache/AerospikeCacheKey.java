package org.springframework.data.aerospike.cache;

import com.aerospike.client.Value;
import lombok.Getter;

/**
 * Wrapper class used in caching. Receives hash of the cache key as a String, a long number or a byte array.
 */
public class AerospikeCacheKey {

    @Getter
    private final Value value;

    private AerospikeCacheKey(String string) {
        this.value = new Value.StringValue(string);
    }

    private AerospikeCacheKey(long number) {
        this.value = new Value.LongValue(number);
    }

    private AerospikeCacheKey(byte[] data) {
        this.value = new Value.BytesValue(data);
    }

    /**
     * Instantiate AerospikeCacheKey instance with a String.
     *
     * @param string String parameter
     * @return new instance of AerospikeCacheKey initialized with the given parameter
     */
    public static AerospikeCacheKey of(String string) {
        return new AerospikeCacheKey(string);
    }

    /**
     * Instantiate AerospikeCacheKey instance with a long number.
     *
     * @param number long number
     * @return new instance of AerospikeCacheKey initialized with the given parameter
     */
    public static AerospikeCacheKey of(long number) {
        return new AerospikeCacheKey(number);
    }

    /**
     * Instantiate AerospikeCacheKey instance with a byte array.
     *
     * @param data byte array
     * @return new instance of AerospikeCacheKey initialized with the given parameter
     */
    public static AerospikeCacheKey of(byte[] data) {
        return new AerospikeCacheKey(data);
    }

}
