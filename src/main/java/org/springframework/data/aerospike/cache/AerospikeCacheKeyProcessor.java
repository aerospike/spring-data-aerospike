package org.springframework.data.aerospike.cache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import net.jpountz.xxhash.XXHash64;
import net.jpountz.xxhash.XXHashFactory;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.nio.ByteBuffer;

public class AerospikeCacheKeyProcessor {

    private static final Kryo kryoInstance = new Kryo();

    public AerospikeCacheKeyProcessor() {
        configureKryo();
    }

    /**
     * Configuration for Kryo instance.
     * <p>
     * Classes of the objects to be cached can be pre-registered if required. Registering in advance is not necessary,
     * however it can be done to increase serialization performance. If a class has been pre-registered, the first time
     * it is encountered Kryo can just output a numeric reference to it instead of writing fully qualified class name.
     */
    public void configureKryo() {
        // setting to false means not requiring registration for all the classes of cached objects in advance
        kryoInstance.setRegistrationRequired(false);
        kryoInstance.setInstantiatorStrategy(new StdInstantiatorStrategy());
    }

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
     * <p>
     * The method can be overridden if different serialization implementation is required.
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
     * The default implementation is 64 bit xxHash.
     * <p>
     * The method can be overridden if different hashing algorithm or implementation is required.
     *
     * @param data Byte array to be hashed
     * @return AerospikeCacheKey instantiated with either a String or a long number
     */
    public static AerospikeCacheKey calculateHash(byte[] data) {
        XXHash64 xxHash64 = XXHashFactory.fastestInstance().hash64();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        return AerospikeCacheKey.of(xxHash64.hash(buffer, 0, data.length, 0));
    }
}
