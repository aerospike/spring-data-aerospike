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

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.aerospike.repository.query.AerospikeQueryCreator;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;

/**
 * @author Oliver Gierke
 * @author Igor Ermolenko
 */
public abstract class BaseAerospikeRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

    protected static final String MAPPING_CONTEXT_BEAN_NAME = "aerospikeMappingContext";
    protected static final String AEROSPIKE_TEMPLATE_BEAN_REF_ATTRIBUTE = "aerospikeTemplateRef";

    @Override
    public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

        AnnotationAttributes attributes = config.getAttributes();

        builder.addPropertyReference("template", attributes.getString(AEROSPIKE_TEMPLATE_BEAN_REF_ATTRIBUTE));
        builder.addPropertyValue("queryCreator", AerospikeQueryCreator.class);
        builder.addPropertyReference("mappingContext", MAPPING_CONTEXT_BEAN_NAME);
    }
}
