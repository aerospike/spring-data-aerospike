package org.springframework.data.aerospike.config;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
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
import org.springframework.data.aerospike.util.AdditionalAerospikeTestOperations;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * @author Peter Milne
 * @author Jean Mercier
 */
@EnableReactiveAerospikeRepositories(basePackageClasses = {ReactiveCustomerRepository.class})
public class ReactiveTestConfig extends AbstractReactiveAerospikeDataConfiguration {

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
    protected EventLoops eventLoops() {
        int nThreads = Math.max(2, Runtime.getRuntime().availableProcessors() * 2);
        String os = System.getProperty("os.name").toLowerCase(Locale.ENGLISH);

        EventLoopGroup eventLoopGroup;
        if (os.contains("nux") && Epoll.isAvailable()) {
            eventLoopGroup = new EpollEventLoopGroup(nThreads);
        } else if (os.contains("mac") && KQueue.isAvailable()) {
            eventLoopGroup = new KQueueEventLoopGroup(nThreads);
        } else {
            eventLoopGroup = new NioEventLoopGroup(nThreads);
        }

        EventPolicy eventPolicy = new EventPolicy();
        eventPolicy.maxCommandsInProcess = 40;
        eventPolicy.maxCommandsInQueue = 1024;
        return new NettyEventLoops(eventPolicy, eventLoopGroup);
    }

    @Bean
    public AdditionalAerospikeTestOperations aerospikeOperations(ReactiveAerospikeTemplate template,
                                                                 IAerospikeClient client,
                                                                 GenericContainer<?> aerospike,
                                                                 ServerVersionSupport serverVersionSupport) {
        return new ReactiveBlockingAerospikeTestOperations(new IndexInfoParser(), client, aerospike, template,
            serverVersionSupport);
    }
}
