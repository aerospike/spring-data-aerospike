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
import com.aerospike.client.policy.ClientPolicy;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.aerospike.convert.AerospikeCustomConversions;
import org.springframework.data.aerospike.convert.AerospikeTypeAliasAccessor;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.core.AerospikeExceptionTranslator;
import org.springframework.data.aerospike.core.DefaultAerospikeExceptionTranslator;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikeSimpleTypes;
import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.query.FilterExpressionsBuilder;
import org.springframework.data.aerospike.query.StatementBuilder;
import org.springframework.data.aerospike.query.cache.IndexesCache;
import org.springframework.data.aerospike.query.cache.IndexesCacheHolder;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * @author Taras Danylchuk
 */
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
                                                               AerospikeCustomConversions customConversions) {
        return new MappingAerospikeConverter(aerospikeMappingContext, customConversions, aerospikeTypeAliasAccessor);
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
    public AerospikeMappingContext aerospikeMappingContext() throws Exception {
        AerospikeMappingContext context = new AerospikeMappingContext();
        context.setInitialEntitySet(getInitialEntitySet());
        context.setSimpleTypeHolder(AerospikeSimpleTypes.HOLDER);
        context.setFieldNamingStrategy(fieldNamingStrategy());
        context.setDefaultNameSpace(nameSpace());
        context.setCreateIndexesOnStartup(isCreateIndexesOnStartup());
        return context;
    }

    @Bean(name = "aerospikeExceptionTranslator")
    public AerospikeExceptionTranslator aerospikeExceptionTranslator() {
        return new DefaultAerospikeExceptionTranslator();
    }

    @Bean(name = "aerospikeClient", destroyMethod = "close")
    public AerospikeClient aerospikeClient() {
        Collection<Host> hosts = getHosts();
        return new AerospikeClient(getClientPolicy(), hosts.toArray(new Host[0]));
    }

    @Bean(name = "filterExpressionsBuilder")
    public FilterExpressionsBuilder filterExpressionsBuilder() {
        return new FilterExpressionsBuilder();
    }

    protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {
        String basePackage = getMappingBasePackage();
        Set<Class<?>> initialEntitySet = new HashSet<>();

        if (StringUtils.hasText(basePackage)) {
            ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(false);
            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
            componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));
            for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
                initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(), AerospikeDataConfigurationSupport.class.getClassLoader()));
            }
        }

        return initialEntitySet;
    }

    protected String getMappingBasePackage() {
        return getClass().getPackage().getName();
    }

    protected FieldNamingStrategy fieldNamingStrategy() {
        return PropertyNameFieldNamingStrategy.INSTANCE;
    }

    protected abstract Collection<Host> getHosts();

    protected abstract String nameSpace();

    protected boolean isCreateIndexesOnStartup() {
        return true;
    }

    protected AerospikeDataSettings aerospikeDataSettings() {
        AerospikeDataSettings.AerospikeDataSettingsBuilder builder = AerospikeDataSettings.builder();
        configureDataSettings(builder);
        return builder.build();
    }

    protected void configureDataSettings(AerospikeDataSettings.AerospikeDataSettingsBuilder builder) {
        builder.scansEnabled(false);
        builder.sendKey(true);
    }

    protected ClientPolicy getClientPolicy() {
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.failIfNotConnected = true;
        clientPolicy.timeout = 10_000;
        clientPolicy.writePolicyDefault.sendKey = aerospikeDataSettings().isSendKey();
        return clientPolicy;
    }
}
