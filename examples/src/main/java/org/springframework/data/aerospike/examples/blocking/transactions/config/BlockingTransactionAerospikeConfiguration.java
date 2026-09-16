package org.springframework.data.aerospike.examples.blocking.transactions.config;

import com.aerospike.client.IAerospikeClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.aerospike.config.AbstractAerospikeDataConfiguration;
import org.springframework.data.aerospike.config.AerospikeDataSettings;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.examples.blocking.transactions.BlockingTransactionExample;
import org.springframework.data.aerospike.examples.blocking.transactions.BlockingTransactionalMovieService;
import org.springframework.data.aerospike.examples.blocking.transactions.entity.BlockingTransactionalMovieDocument;
import org.springframework.data.aerospike.examples.blocking.transactions.repository.BlockingTransactionalMovieRepository;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.aerospike.transaction.sync.AerospikeTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

// tag::transactions-blocking-configuration[]
@Configuration(proxyBeanMethods = false)
@PropertySource("classpath:examples-application.properties")
@EnableAerospikeRepositories(basePackageClasses = BlockingTransactionalMovieRepository.class)
@EnableTransactionManagement
// Enables repository proxies and Spring transaction interception for blocking transaction examples.
public class BlockingTransactionAerospikeConfiguration extends AbstractAerospikeDataConfiguration {

    @Bean
    BlockingTransactionalMovieService blockingTransactionalMovieService(
        BlockingTransactionalMovieRepository repository, AerospikeTemplate template) {
        return new BlockingTransactionalMovieService(repository, template);
    }

    @Bean
    BlockingTransactionExample blockingTransactionExample(BlockingTransactionalMovieRepository repository,
                                                          BlockingTransactionalMovieService service,
                                                          ServerVersionSupport serverVersionSupport) {
        return new BlockingTransactionExample(repository, service, serverVersionSupport);
    }

    @Override
    protected String getMappingBasePackage() {
        return BlockingTransactionalMovieDocument.class.getPackageName();
    }

    @Override
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
        aerospikeDataSettings.setCreateIndexesOnStartup(false);
        aerospikeDataSettings.setScansEnabled(true);
    }

    // tag::transactions-blocking-manager[]
    // Spring uses this manager for @Transactional blocking Aerospike operations.
    @Bean
    public AerospikeTransactionManager aerospikeTransactionManager(IAerospikeClient client) {
        return new AerospikeTransactionManager(client);
    }
    // end::transactions-blocking-manager[]
}
// end::transactions-blocking-configuration[]
