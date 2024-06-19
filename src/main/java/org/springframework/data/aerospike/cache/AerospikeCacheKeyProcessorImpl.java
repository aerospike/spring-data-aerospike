package org.springframework.data.aerospike.cache;

import org.objenesis.strategy.StdInstantiatorStrategy;

public class AerospikeCacheKeyProcessorImpl extends AerospikeCacheKeyProcessor {

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
}
