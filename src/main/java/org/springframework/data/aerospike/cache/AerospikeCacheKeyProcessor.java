package org.springframework.data.aerospike.cache;

interface AerospikeCacheKeyProcessor {

    /**
     * Serialize the given key and calculate hash based on the serialization result.
     *
     * @param key Object to be serialized and hashed
     * @return AerospikeCacheKey instantiated with either a String or a long number
     */
    AerospikeCacheKey serializeAndHash(Object key);

    /**
     * Serialize the given key.
     * <p>
     * The default implementation uses Kryo.
     *
     * @param key Object to be serialized
     * @return byte[]
     */
    byte[] serialize(Object key);

    /**
     * Calculate hash based on the given byte array.
     * <p>
     * The default implementation uses 128 bit Murmur3 hashing.
     *
     * @param data Byte array to be hashed
     * @return AerospikeCacheKey instantiated with either a String or a long number
     */
    AerospikeCacheKey calculateHash(byte[] data);
}
