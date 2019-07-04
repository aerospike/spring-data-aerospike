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
import org.springframework.data.aerospike.cache.AerospikeCacheManager;
import org.springframework.data.aerospike.cache.AerospikeCacheManagerIntegrationTests.CachingComponent;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.repository.config.EnableAerospikeRepositories;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;
import org.springframework.data.aerospike.sample.ContactRepository;
import org.springframework.data.aerospike.sample.CustomerRepository;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;

import java.util.Collection;
import java.util.Collections;
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
    }
}
