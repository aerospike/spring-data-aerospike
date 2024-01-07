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
import com.aerospike.client.policy.ClientPolicy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
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
import java.util.Set;

/**
 * @author Taras Danylchuk
 */
@Slf4j
@Configuration
public abstract class AerospikeDataConfigurationSupport {

    @Bean(name = "aerospikeStatementBuilder")
    public StatementBuilder statementBuilder(IndexesCache indexesCache) {
        return new StatementBuilder(indexesCache);
    }

    @Bean(name = "aerospikeIndexCache")
    public IndexesCacheHolder indexCache() {
        return new IndexesCacheHolder();
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
    public AerospikeClient aerospikeClient(AerospikeSettings settings) {
        return new AerospikeClient(getClientPolicy(), settings.getConnectionSettings().getHostsArray());
    }

    protected int getDefaultPort() {
        return 3000;
    }

    @Bean(name = "filterExpressionsBuilder")
    public FilterExpressionsBuilder filterExpressionsBuilder() {
        return new FilterExpressionsBuilder();
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
     * <p>The return value of this method overrides the values of 'spring-data-aerospike.data.*' parameters
     * from application.properties.
     *
     * @return Collection of Host objects for Aerospike client to connect
     */
    protected AerospikeDataSettings configureDataSettings() {
        return null;
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
        clientPolicy.writePolicyDefault.sendKey = true;
        clientPolicy.readPolicyDefault.sendKey = true;
        log.debug("AerospikeDataSettings.sendKey: {}", clientPolicy.writePolicyDefault.sendKey);
        return clientPolicy;
    }

    @Bean
    @ConfigurationProperties(prefix = "spring-data-aerospike.data")
    public AerospikeDataSettings readAerospikeDataSettings() {
        return new AerospikeDataSettings();
    }

    @Bean
    @ConfigurationProperties(prefix = "spring-data-aerospike.connection")
    public AerospikeConnectionSettings readAerospikeSettings() {
        return new AerospikeConnectionSettings();
    }

    @Bean
    protected AerospikeSettings aerospikeDataSettings(AerospikeDataSettings dataSettings,
                                                      AerospikeConnectionSettings connectionSettings) {
        AerospikeDataSettings manualDataSettings = configureDataSettings();
        // configureDataSettings() return value has precedence over the parameters from application.properties
        if (manualDataSettings != null) {
            dataSettings = manualDataSettings;
        }

        // getHosts() return value has precedence over hosts parameter from application.properties
        if (getHosts() != null) {
            connectionSettings.setHostsArray(getHosts().toArray(new Host[0]));
        } else {
            connectionSettings.setHostsArray(Host.parseHosts(connectionSettings.getHosts(), getDefaultPort()));
        }

        // nameSpace() return value has precedence over namespace parameter from application.properties
        if (nameSpace() != null) connectionSettings.setNamespace(nameSpace());

        return new AerospikeSettings(connectionSettings, dataSettings);
    }
}
