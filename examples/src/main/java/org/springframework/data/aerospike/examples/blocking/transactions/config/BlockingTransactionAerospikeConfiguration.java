package org.springframework.data.aerospike.examples.blocking.transactions.config;

import com.aerospike.client.IAerospikeClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.examples.blocking.transactions.BlockingTransactionExample;
import org.springframework.data.aerospike.examples.blocking.transactions.entity.BlockingTransactionalMovieDocument;
import org.springframework.data.aerospike.examples.blocking.transactions.repository.BlockingTransactionalMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.transaction.sync.AerospikeTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@PropertySource("classpath:application.properties")
@ComponentScan(basePackageClasses = BlockingTransactionExample.class)
@EnableAerospikeRepositories(basePackageClasses = BlockingTransactionalMovieRepository.class)
@EnableTransactionManagement
public class BlockingTransactionAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Override
    protected String getMappingBasePackage() {
        return BlockingTransactionalMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }

    @Bean
    public AerospikeTransactionManager aerospikeTransactionManager(IAerospikeClient client) {
        return new AerospikeTransactionManager(client);
    }
}
