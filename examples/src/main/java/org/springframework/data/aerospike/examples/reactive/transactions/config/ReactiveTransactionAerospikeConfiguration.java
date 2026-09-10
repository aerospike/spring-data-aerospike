package org.springframework.data.aerospike.examples.reactive.transactions.config;

import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractReactiveAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.reactive.transactions.ReactiveTransactionExample;
import org.springframework.data.aerospike.examples.reactive.transactions.entity.ReactiveTransactionalMovieDocument;
import org.springframework.data.aerospike.examples.reactive.transactions.repository.ReactiveTransactionalMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;
import org.springframework.data.aerospike.transaction.reactive.AerospikeReactiveTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = ReactiveTransactionExample.class)
@EnableReactiveAerospikeRepositories(basePackageClasses = ReactiveTransactionalMovieRepository.class)
@EnableTransactionManagement
public class ReactiveTransactionAerospikeConfiguration extends AbstractReactiveAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return ReactiveTransactionalMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }

    @Bean
    public AerospikeReactiveTransactionManager aerospikeReactiveTransactionManager(IAerospikeReactorClient client) {
        return new AerospikeReactiveTransactionManager(client);
    }

    @Bean
    public TransactionalOperator transactionalOperator(AerospikeReactiveTransactionManager transactionManager) {
        return TransactionalOperator.create(transactionManager, new DefaultTransactionDefinition());
    }
}
