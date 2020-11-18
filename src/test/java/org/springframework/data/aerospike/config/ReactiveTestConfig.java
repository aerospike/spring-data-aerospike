package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.NioEventLoops;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.aerospike.AdditionalAerospikeTestOperations;
import org.springframework.data.aerospike.ReactiveBlockingAerospikeTestOperations;
import org.springframework.data.aerospike.SampleClasses;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.repository.config.EnableReactiveAerospikeRepositories;
import org.springframework.data.aerospike.sample.ReactiveCustomerRepository;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@EnableReactiveAerospikeRepositories(basePackageClasses = {ReactiveCustomerRepository.class})
public class ReactiveTestConfig extends AbstractReactiveAerospikeDataConfiguration {

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

    @Override
    protected void configureDataSettings(AerospikeDataSettings.AerospikeDataSettingsBuilder builder) {
        builder.scansEnabled(true);
    }

    @Bean
    public AdditionalAerospikeTestOperations aerospikeOperations(ReactiveAerospikeTemplate template,
                                                                 AerospikeClient client,
                                                                 GenericContainer aerospike) {
        return new ReactiveBlockingAerospikeTestOperations(client, aerospike, template);
    }
}
