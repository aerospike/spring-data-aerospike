package org.springframework.data.aerospike.cache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import lombok.Getter;
import org.apache.commons.codec.digest.MurmurHash3;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.util.Arrays;

public class AerospikeCacheKeyProcessorImpl implements AerospikeCacheKeyProcessor {

    @Getter
    private static final Kryo kryoInstance = new Kryo();

    public AerospikeCacheKeyProcessorImpl() {
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
        getKryoInstance().setRegistrationRequired(false);
        getKryoInstance().setInstantiatorStrategy(new StdInstantiatorStrategy());
    }

    public AerospikeCacheKey serializeAndHash(Object key) {
        return calculateHash(serialize(key));
    }

    public byte[] serialize(Object key) {
        ByteBufferOutput output = new ByteBufferOutput(1024); // Initial buffer size
        kryoInstance.writeClassAndObject(output, key);
        output.flush();
        return output.toBytes();
    }

    public AerospikeCacheKey calculateHash(byte[] data) {
        return AerospikeCacheKey.of(Arrays.toString(MurmurHash3.hash128(data)));
    }
}
