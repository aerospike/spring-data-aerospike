/*
 * Copyright 2015 the original author or authors.
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

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.data.config.ParsingUtils;
import org.w3c.dom.Element;

/**
 * {@link BeanDefinitionParser} to create a {@link BeanDefinition} for a {@link QueryPolicyFactoryBean}.
 *
 * @author Peter Milne
 */
public class QueryPolicyBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    @Override
    protected Class<?> getBeanClass(Element element) {
        return QueryPolicyFactoryBean.class;
    }

    @Override
    protected void doParse(Element element, BeanDefinitionBuilder builder) {
        ParsingUtils.setPropertyValue(builder, element, "max-concurrent-threads");
        // TODO: set all properties of the XML element on the builder
    }
}
