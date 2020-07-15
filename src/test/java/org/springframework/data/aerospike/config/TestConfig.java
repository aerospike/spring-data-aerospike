package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NioEventLoops;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.aerospike.BlockingAerospikeTestOperations;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.cache.AerospikeCacheManager;
import org.springframework.data.aerospike.cache.AerospikeCacheManagerIntegrationTests.CachingComponent;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.query.QueryEngineTestDataPopulator;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;
import org.springframework.data.aerospike.sample.ContactRepository;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 *
 *
 * @author Peter Milne
 * @author Jean Mercier
 *
 */
@Configuration
@EnableAerospikeRepositories(basePackageClasses = {ContactRepository.class, CustomerRepository.class})
@EnableReactiveAerospikeRepositories(basePackageClasses = {ReactiveCustomerRepository.class})
@EnableCaching
@EnableAutoConfiguration
public class TestConfig extends AbstractReactiveAerospikeDataConfiguration  {

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

	@Bean
	public CacheManager cacheManager(AerospikeClient aerospikeClient, MappingAerospikeConverter aerospikeConverter) {
		return new AerospikeCacheManager(aerospikeClient, aerospikeConverter);
	}

	@Bean
	public CachingComponent cachingComponent() {
		return new CachingComponent();
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
    protected EventLoops eventLoops() {
        return new NioEventLoops();
// TODO: support parameterized EventLoopType
//
//		case DIRECT_NIO: {
//			return new NioEventLoops(1);
//		}
//
//		case NETTY_NIO: {
//			EventLoopGroup group = new NioEventLoopGroup(1);
//			return new NettyEventLoops(group);
//		}
//
//		case NETTY_EPOLL: {
//			EventLoopGroup group = new EpollEventLoopGroup(1);
//			return new NettyEventLoops(group);
//		}
    }

    @Bean
	BlockingAerospikeTestOperations blockingAerospikeTestOperations(AerospikeTemplate template, AerospikeClient client,
																	GenericContainer aerospike) {
		return new BlockingAerospikeTestOperations(template, client, aerospike);
	}

	@Bean
	public QueryEngineTestDataPopulator queryEngineTestDataPopulator(AerospikeClient client) {
		return new QueryEngineTestDataPopulator(nameSpace(), client);
	}

	@Override
	protected void configureDataSettings(AerospikeDataSettings.AerospikeDataSettingsBuilder builder) {
		builder.scansEnabled(true);
	}
}
