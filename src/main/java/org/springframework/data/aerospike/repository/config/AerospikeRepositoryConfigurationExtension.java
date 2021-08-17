/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.repository.config;

import org.springframework.data.aerospike.mapping.Document;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.aerospike.repository.support.AerospikeRepositoryFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.core.RepositoryMetadata;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;

/**
 * {@link RepositoryConfigurationExtension} for Aerospike.
 *
 * @author Oliver Gierke
 */
public class AerospikeRepositoryConfigurationExtension extends BaseAerospikeRepositoryConfigurationExtension {

    @Override
    public String getModuleName() {
        return "Aerospike";
    }

    @Override
    protected String getModulePrefix() {
        return "aerospike";
    }

    @Override
    public String getRepositoryFactoryBeanClassName() {
        return AerospikeRepositoryFactoryBean.class.getName();
    }

    @Override
    protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
        return Collections.singleton(Document.class);
    }

    @Override
    protected Collection<Class<?>> getIdentifyingTypes() {
        return Collections.singleton(AerospikeRepository.class);
    }

    @Override
    protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
        return !metadata.isReactiveRepository();
    }
}
