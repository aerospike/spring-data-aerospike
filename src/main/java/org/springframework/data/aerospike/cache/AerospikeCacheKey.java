package org.springframework.data.aerospike.cache;

import com.aerospike.client.Value;
import lombok.Getter;

public class AerospikeCacheKey {

    @Getter
    private Value value;

    private AerospikeCacheKey(String string) {
        this.value = new Value.StringValue(string);
    }

    private AerospikeCacheKey(long number) {
        this.value = new Value.LongValue(number);
    }

    /**
     * Instantiate AerospikeCacheKey instance with a String.
     *
     * @param string String parameter
     * @return AerospikeCacheKey
     */
    public static AerospikeCacheKey of(String string) {
        return new AerospikeCacheKey(string);
    }

    /**
     * Instantiate AerospikeCacheKey instance with a long number.
     *
     * @param number long number
     * @return AerospikeCacheKey
     */
    public static AerospikeCacheKey of(long number) {
        return new AerospikeCacheKey(number);
    }
}
