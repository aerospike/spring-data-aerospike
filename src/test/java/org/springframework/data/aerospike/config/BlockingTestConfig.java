package org.springframework.data.aerospike.config;

import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.aerospike.AdditionalAerospikeTestOperations;
import org.springframework.data.aerospike.BlockingAerospikeTestOperations;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.sample.ContactRepository;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@EnableAerospikeRepositories(basePackageClasses = {ContactRepository.class, CustomerRepository.class})
public class BlockingTestConfig extends AbstractAerospikeDataConfiguration {

    @Value("${embedded.aerospike.namespace}")
    protected String namespace;
    @Value("${embedded.aerospike.host}")
    protected String host;
    @Value("${embedded.aerospike.port}")
    protected int port;

    @Override
    protected List<?> customConverters() {
        return Arrays.asList(
            SampleClasses.CompositeKey.CompositeKeyToStringConverter.INSTANCE,
            SampleClasses.CompositeKey.StringToCompositeKeyConverter.INSTANCE
        );
    }

    @Override
    protected Collection<Host> getHosts() {
        return Collections.singleton(new Host(host, port));
    }

    @Override
    protected String nameSpace() {
        return namespace;
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings.AerospikeDataSettingsBuilder builder) {
        builder.scansEnabled(true);
    }

    @Override
    protected ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = super.getClientPolicy(); // applying default values first
        clientPolicy.readPolicyDefault.maxRetries = 3;
        clientPolicy.writePolicyDefault.totalTimeout = 1000;
        clientPolicy.infoPolicyDefault.timeout = 1000;
        return clientPolicy;
    }

    @Bean
    public AdditionalAerospikeTestOperations aerospikeOperations(AerospikeTemplate template, IAerospikeClient client,
                                                                 GenericContainer<?> aerospike) {
        return new BlockingAerospikeTestOperations(new IndexInfoParser(), template, client, aerospike);
    }
}
