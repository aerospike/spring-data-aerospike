package org.springframework.data.aerospike.config;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.BlockingAerospikeTestOperations;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.sample.ContactRepository;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.util.AdditionalAerospikeTestOperations;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.List;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@EnableAerospikeRepositories(basePackageClasses = {ContactRepository.class, CustomerRepository.class})
public class BlockingTestConfig extends AbstractAerospikeDataConfiguration {

    @Autowired
    Environment env;

    @Override
    protected List<?> customConverters() {
        return Arrays.asList(
            SampleClasses.CompositeKey.CompositeKeyToStringConverter.INSTANCE,
            SampleClasses.CompositeKey.StringToCompositeKeyConverter.INSTANCE
        );
    }

    @Override
    protected ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = super.getClientPolicy(); // applying default values first
        int totalTimeout = 2000;
        clientPolicy.readPolicyDefault.totalTimeout = totalTimeout;
        clientPolicy.writePolicyDefault.totalTimeout = totalTimeout;
        clientPolicy.batchPolicyDefault.totalTimeout = totalTimeout;
        clientPolicy.infoPolicyDefault.timeout = totalTimeout;
        clientPolicy.readPolicyDefault.maxRetries = 3;
        return clientPolicy;
    }

    @Bean
    public AdditionalAerospikeTestOperations aerospikeOperations(AerospikeTemplate template, IAerospikeClient client,
                                                                 GenericContainer<?> aerospike,
                                                                 ServerVersionSupport serverVersionSupport) {
        return new BlockingAerospikeTestOperations(new IndexInfoParser(), template, client, aerospike,
            serverVersionSupport);
    }

    @Bean
    public IndexedBinsAnnotationsProcessor someAnnotationProcessor() {
        return new IndexedBinsAnnotationsProcessor();
    }
}
