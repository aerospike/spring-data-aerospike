package org.springframework.data.aerospike.config;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import com.playtika.testcontainer.aerospike.AerospikeExpiredDocumentsCleaner;
import com.playtika.testcontainer.aerospike.AerospikeTestOperations;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.data.aerospike.ReactiveBlockingAerospikeTestOperations;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.query.cache.IndexInfoParser;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.springframework.data.aerospike.sample.SampleClasses;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.transaction.reactive.AerospikeReactiveTransactionManager;
import org.springframework.data.aerospike.util.AdditionalAerospikeTestOperations;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.testcontainers.containers.GenericContainer;

import java.util.List;
import java.util.Objects;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@EnableReactiveAerospikeRepositories(basePackageClasses = {ReactiveCustomerRepository.class})
@EnableTransactionManagement
public class ReactiveTestConfig extends AbstractReactiveAerospikeDataConfiguration {

    @Autowired
    Environment env;

    @Override
    protected List<Object> customConverters() {
        return List.of(
            SampleClasses.CompositeKey.CompositeKeyToStringConverter.INSTANCE,
            SampleClasses.CompositeKey.StringToCompositeKeyConverter.INSTANCE
        );
    }

    @Bean
    public AdditionalAerospikeTestOperations aerospikeOperations(ReactiveAerospikeTemplate template,
                                                                 IAerospikeClient client,
                                                                 ObjectProvider<GenericContainer<?>> containerObjectProvider,
                                                                 ServerVersionSupport serverVersionSupport) {
        GenericContainer<?> container = containerObjectProvider.getIfAvailable();
        return new ReactiveBlockingAerospikeTestOperations(new IndexInfoParser(), client, container, template,
            serverVersionSupport);
    }

    @Bean
    public AerospikeTestOperations aerospikeTestOperations(ObjectProvider<GenericContainer<?>> containerObjectProvider, IAerospikeClient client) {
        GenericContainer<?> container = containerObjectProvider.getIfAvailable();
        return new AerospikeTestOperations(
            new AerospikeExpiredDocumentsCleaner(client, "test"),
            container
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
    public ReactiveTransactionManager aerospikeReactiveTransactionManager(IAerospikeReactorClient client) {
        return new AerospikeReactiveTransactionManager(client);
    }

    @Bean(name = "reactiveTransactionalOperator")
    public TransactionalOperator reactiveTransactionalOperator(
        AerospikeReactiveTransactionManager reactiveTransactionManager
    ) {
        return TransactionalOperator.create(reactiveTransactionManager, new DefaultTransactionDefinition());
    }

    @Bean(name = "reactiveTransactionalOperatorWithTimeout2")
    public TransactionalOperator reactiveTransactionalOperatorWithTimeout2(
        AerospikeReactiveTransactionManager reactiveTransactionManager
    ) {
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setTimeout(2);
        return TransactionalOperator.create(reactiveTransactionManager, definition);
    }
}
