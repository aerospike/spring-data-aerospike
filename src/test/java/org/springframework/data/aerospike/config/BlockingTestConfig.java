package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.playtika.testcontainer.aerospike.AerospikeExpiredDocumentsCleaner;
import com.playtika.testcontainer.aerospike.AerospikeTestOperations;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.data.aerospike.BlockingAerospikeTestOperations;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.sample.ContactRepository;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.transaction.sync.AerospikeTransactionManager;
import org.springframework.data.aerospike.util.AdditionalAerospikeTestOperations;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.GenericContainer;

import java.util.List;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@EnableAerospikeRepositories(basePackageClasses = {ContactRepository.class, CustomerRepository.class})
@EnableTransactionManagement
public class BlockingTestConfig extends AbstractAerospikeDataConfiguration {

    @Override
    protected List<Object> customConverters() {
        return List.of(
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
                                                                 ObjectProvider<GenericContainer<?>> containerObjectProvider,
                                                                 ServerVersionSupport serverVersionSupport) {
        GenericContainer<?> container = containerObjectProvider.getIfAvailable();
        return new BlockingAerospikeTestOperations(new IndexInfoParser(), template, client, container,
            serverVersionSupport);
    }

    @Override
    @Bean(name = "aerospikeClient", destroyMethod = "close")
    public IAerospikeClient aerospikeClient(AerospikeSettings settings) {
        return new AerospikeClient(getClientPolicy(), settings.getConnectionSettings().getHostsArray());
    }

    @Bean
    public IndexedBinsAnnotationsProcessor someAnnotationProcessor() {
        return new IndexedBinsAnnotationsProcessor();
    }

    @Bean
    public AerospikeTransactionManager aerospikeTransactionManager(IAerospikeClient client) {
        return new AerospikeTransactionManager(client);
    }

    @Bean
    public TransactionTemplate transactionTemplate(AerospikeTransactionManager transactionManager) {
        return new TransactionTemplate(transactionManager);
    }
}
