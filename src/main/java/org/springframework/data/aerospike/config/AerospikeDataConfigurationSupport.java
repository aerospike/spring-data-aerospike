/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.config;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.async.EventLoops;
import com.aerospike.client.async.EventPolicy;
import com.aerospike.client.async.NettyEventLoops;
import com.aerospike.client.policy.ClientPolicy;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.aerospike.cache.AerospikeCacheKeyProcessor;
import org.springframework.data.aerospike.cache.AerospikeCacheKeyProcessorImpl;
import org.springframework.data.aerospike.convert.AerospikeCustomConversions;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.DefaultAerospikeExceptionTranslator;
import org.springframework.data.aerospike.index.AerospikeIndexResolver;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikeSimpleTypes;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.query.FilterExpressionsBuilder;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.cache.IndexesCacheHolder;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * @author Taras Danylchuk
 */
@Slf4j
@Configuration
public abstract class AerospikeDataConfigurationSupport {

    public static final String CONFIG_PREFIX = "spring.aerospike";
    public static final String CONFIG_PREFIX_DATA = CONFIG_PREFIX + ".data";
    public static final String CONFIG_PREFIX_CONNECTION = CONFIG_PREFIX + ".connection";

    @Bean(name = "aerospikeStatementBuilder")
    public StatementBuilder statementBuilder(IndexesCache indexesCache) {
        return new StatementBuilder(indexesCache);
    }

    @Bean(name = "aerospikeIndexCache")
    public IndexesCacheHolder indexCache() {
        return new IndexesCacheHolder();
    }

    @Bean(name = "aerospikeCacheKeyProcessor")
    public AerospikeCacheKeyProcessor cacheKeyProcessor() {
        return new AerospikeCacheKeyProcessorImpl();
    }

    @Bean(name = "mappingAerospikeConverter")
    public MappingAerospikeConverter mappingAerospikeConverter(AerospikeMappingContext aerospikeMappingContext,
                                                               AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor,
                                                               AerospikeCustomConversions customConversions,
                                                               AerospikeSettings settings) {
        return new MappingAerospikeConverter(aerospikeMappingContext, customConversions, aerospikeTypeAliasAccessor,
            settings.getDataSettings());
    }

    @Bean(name = "aerospikeTypeAliasAccessor")
    public AerospikeTypeAliasAccessor aerospikeTypeAliasAccessor() {
        return new AerospikeTypeAliasAccessor();
    }

    @Bean(name = "aerospikeCustomConversions")
    public AerospikeCustomConversions customConversions() {
        return new AerospikeCustomConversions(customConverters());
    }

    protected List<?> customConverters() {
        return Collections.emptyList();
    }

    @Bean(name = "aerospikeMappingContext")
    public AerospikeMappingContext aerospikeMappingContext() throws ClassNotFoundException {
        AerospikeMappingContext context = new AerospikeMappingContext();
        context.setInitialEntitySet(getInitialEntitySet());
        context.setSimpleTypeHolder(AerospikeSimpleTypes.HOLDER);
        context.setFieldNamingStrategy(fieldNamingStrategy());
        return context;
    }

    @Bean(name = "aerospikeExceptionTranslator")
    public AerospikeExceptionTranslator aerospikeExceptionTranslator() {
        return new DefaultAerospikeExceptionTranslator();
    }

    @Bean(name = "aerospikeClient", destroyMethod = "close")
    public IAerospikeClient aerospikeClient(AerospikeSettings settings) {
        // another implementation of IAerospikeClient can be instantiated here by overriding the bean
        return new AerospikeClient(getClientPolicy(), settings.getConnectionSettings().getHostsArray());
    }

    protected int getDefaultPort() {
        return 3000;
    }

    @Bean(name = "filterExpressionsBuilder")
    public FilterExpressionsBuilder filterExpressionsBuilder() {
        return new FilterExpressionsBuilder();
    }

    @Bean
    public EventLoops eventLoops() {
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

    @Bean(name = "aerospikeIndexResolver")
    public AerospikeIndexResolver aerospikeIndexResolver() {
        return new AerospikeIndexResolver();
    }

    @Bean(name = "aerospikeServerVersionSupport")
    public ServerVersionSupport serverVersionSupport(IAerospikeClient aerospikeClient, AerospikeSettings settings) {
        ServerVersionSupport serverVersionSupport = new ServerVersionSupport(aerospikeClient);
        int serverVersionRefreshFrequency = settings.getDataSettings().getServerVersionRefreshSeconds();
        processServerVersionRefreshFrequency(serverVersionRefreshFrequency, serverVersionSupport);
        return serverVersionSupport;
    }

    private void processServerVersionRefreshFrequency(int serverVersionRefreshSeconds,
                                                      ServerVersionSupport serverVersionSupport) {
        if (serverVersionRefreshSeconds <= 0) {
            log.info("Periodic server version refreshing is not scheduled, interval ({}) is <= 0",
                serverVersionRefreshSeconds);
        } else {
            serverVersionSupport.scheduleServerVersionRefresh(serverVersionRefreshSeconds);
        }
    }

    protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
        String basePackage = getMappingBasePackage();
        Set<Class<?>> initialEntitySet = new HashSet<>();

        if (StringUtils.hasText(basePackage)) {
            ClassPathScanningCandidateComponentProvider componentProvider =
                new ClassPathScanningCandidateComponentProvider(false);

            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

            for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
                initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(),
                    AerospikeDataConfigurationSupport.class.getClassLoader()));
            }
        }

        return initialEntitySet;
    }

    protected String getMappingBasePackage() {
        return getClass().getPackage().getName();
    }

    @SuppressWarnings("SameReturnValue")
    protected FieldNamingStrategy fieldNamingStrategy() {
        return PropertyNameFieldNamingStrategy.INSTANCE;
    }

    /**
     * Override this method to define the hosts to be used.
     * <p>The return value of this method overrides the value of the 'hosts' parameter from application.properties.
     *
     * @return Collection of Host objects for Aerospike client to connect
     */
    protected Collection<Host> getHosts() {
        return null;
    }

    /**
     * Override this method to define the namespace to be used.
     * <p>The return value of this method overrides the value of the 'namespace' parameter from application.properties.
     *
     * @return Collection of Host objects for Aerospike client to connect
     */
    protected String nameSpace() {
        return null;
    }

    /**
     * Override this method to define data settings to be used.
     *
     * <p>The return value of this method overrides the values of
     * {@link AerospikeDataConfigurationSupport#CONFIG_PREFIX_DATA} parameters from application.properties.
     */
    protected void configureDataSettings(AerospikeDataSettings aerospikeDataSettings) {
    }

    /**
     * Return {@link ClientPolicy} object that contains all client policies.
     *
     * <p>Override this method to set the necessary parameters, </p>
     * <p>call super.getClientPolicy() to apply default values first.</p>
     *
     * @return new ClientPolicy instance
     */
    protected ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.failIfNotConnected = true;
        clientPolicy.timeout = 10_000;
        boolean sendKey = true;
        clientPolicy.readPolicyDefault.sendKey = sendKey;
        clientPolicy.writePolicyDefault.sendKey = sendKey;
        clientPolicy.batchPolicyDefault.sendKey = sendKey;
        clientPolicy.batchWritePolicyDefault.sendKey = sendKey;
        clientPolicy.queryPolicyDefault.sendKey = sendKey;
        clientPolicy.scanPolicyDefault.sendKey = sendKey;
        clientPolicy.eventLoops = eventLoops();
        return clientPolicy;
    }

    @Bean
    public AerospikeDataSettings readAerospikeDataSettings() {
        return new AerospikeDataSettings();
    }

    @Bean
    public AerospikeConnectionSettings readAerospikeSettings() {
        return new AerospikeConnectionSettings();
    }

    @Bean
    protected AerospikeSettings aerospikeSettings(AerospikeDataSettings dataSettings,
                                                  AerospikeConnectionSettings connectionSettings) {
        // values set via configureDataSettings() have precedence over the parameters from application.properties
        configureDataSettings(dataSettings);

        // getHosts() has precedence over hosts parameter from application.properties
        Collection<Host> hosts;
        if ((hosts = getHosts()) != null) {
            connectionSettings.setHostsArray(hosts.toArray(new Host[0]));
        } else if (StringUtils.hasText(connectionSettings.getHosts())) {
            connectionSettings.setHostsArray(Host.parseHosts(connectionSettings.getHosts(), getDefaultPort()));
        } else {
            throw new IllegalStateException("No hosts found, please set hosts parameter in application.properties or " +
                "override getHosts() method");
        }

        // nameSpace() has precedence over namespace parameter from application.properties
        String namespace;
        if ((namespace = nameSpace()) != null) connectionSettings.setNamespace(namespace);

        return new AerospikeSettings(connectionSettings, dataSettings);
    }
}
