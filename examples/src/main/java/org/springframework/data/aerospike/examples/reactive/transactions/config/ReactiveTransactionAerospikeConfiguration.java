package org.springframework.data.aerospike.examples.reactive.transactions.config;

import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.examples.reactive.transactions.ReactiveTransactionExample;
import org.springframework.data.aerospike.examples.reactive.transactions.entity.ReactiveTransactionalMovieDocument;
import org.springframework.data.aerospike.examples.reactive.transactions.repository.ReactiveTransactionalMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.transaction.reactive.AerospikeReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

// tag::transactions-reactive-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveTransactionalMovieRepository.class)
@EnableTransactionManagement
// Enables reactive repository proxies and transaction infrastructure for reactive examples.
public class ReactiveTransactionAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Bean
    ReactiveTransactionExample reactiveTransactionExample(ReactiveTransactionalMovieRepository repository,
                                                          ReactiveAerospikeTemplate template,
                                                          TransactionalOperator transactionalOperator,
                                                          ServerVersionSupport serverVersionSupport) {
        return new ReactiveTransactionExample(repository, template, transactionalOperator, serverVersionSupport);
    }

    @Override
    protected String getMappingBasePackage() {
        return ReactiveTransactionalMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }

    // tag::transactions-reactive-manager[]
    // The reactive transaction manager coordinates Aerospike Reactor operations.
    @Bean
    public AerospikeReactiveTransactionManager aerospikeReactiveTransactionManager(IAerospikeReactorClient client) {
        return new AerospikeReactiveTransactionManager(client);
    }
    // end::transactions-reactive-manager[]

    // tag::transactions-reactive-operator[]
    // TransactionalOperator applies the manager to a reactive publisher chain.
    @Bean
    public TransactionalOperator transactionalOperator(AerospikeReactiveTransactionManager transactionManager) {
        return TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition());
    }
    // end::transactions-reactive-operator[]
}
// end::transactions-reactive-configuration[]
