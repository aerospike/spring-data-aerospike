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

import com.aerospike.client.AerospikeClient;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.config.ParsingUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author Oliver Gierke
 */
public class AerospikeClientBeanDefinitionParser implements BeanDefinitionParser {

    private final ClientPolicyBeanDefinitionParser clientPolicyParser = new ClientPolicyBeanDefinitionParser();

    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(AerospikeClient.class);
        builder.addConstructorArgValue(element.getAttribute("host"));
        builder.addConstructorArgValue(element.getAttribute("port"));
        builder.setDestroyMethodName("close");

        parserContext.getRegistry().registerBeanDefinition("aerospikeClient",
            ParsingUtils.getSourceBeanDefinition(builder, parserContext, element));

        parseNestedClientPolicy(element, parserContext, builder);

        return null;
    }

    /**
     * Looks up a nested {@code client-policy} element within the given one and passes a {@link BeanDefinition} from
     * it.
     *
     * @param element       the current {@code client} element, must not be {@literal null}.
     * @param parserContext must not be {@literal null}.
     * @param builder       must not be {@literal null}.
     */
    private void parseNestedClientPolicy(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        Element clientPolicyElement = DomUtils.getChildElementByTagName(element, "client-policy");

        if (clientPolicyElement != null) {
            BeanDefinition policyDefinition = clientPolicyParser.parse(clientPolicyElement, parserContext);
            builder.addConstructorArgValue(policyDefinition);
        }
    }
}
