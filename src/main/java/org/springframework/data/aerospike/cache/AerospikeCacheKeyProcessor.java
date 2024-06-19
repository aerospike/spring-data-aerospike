package org.springframework.data.aerospike.cache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import lombok.Getter;
import org.apache.commons.codec.digest.MurmurHash3;

import java.util.Arrays;

public abstract class AerospikeCacheKeyProcessor {

    @Getter
    private static final Kryo kryoInstance = new Kryo();

    /**
     * Serialize the given key and calculate hash based on the serialization result.
     *
     * @param key Object to be serialized and hashed
     * @return AerospikeCacheKey instantiated with either a String or a long number
     */
    public AerospikeCacheKey serializeAndHash(Object key) {
        return calculateHash(serialize(key));
    }

    /**
     * Serialize the given key.
     * <p>
     * The default implementation uses Kryo.
     *
     * @param key Object to be serialized
     * @return byte[]
     */
    public byte[] serialize(Object key) {
        ByteBufferOutput output = new ByteBufferOutput(1024); // Initial buffer size
        kryoInstance.writeClassAndObject(output, key);
        output.flush();
        return output.toBytes();
    }

    /**
     * Calculate hash based on the given byte array.
     * <p>
     * The default implementation uses 128 bit Murmur3 hashing.
     *
     * @param data Byte array to be hashed
     * @return AerospikeCacheKey instantiated with either a String or a long number
     */
    public AerospikeCacheKey calculateHash(byte[] data) {
        return AerospikeCacheKey.of(Arrays.toString(MurmurHash3.hash128(data)));
    }
}
