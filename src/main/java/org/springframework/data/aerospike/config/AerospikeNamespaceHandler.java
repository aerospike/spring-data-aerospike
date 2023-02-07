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

import org.springframework.beans.factory.xml.NamespaceHandler;
import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * {@link NamespaceHandler} for the Aerospike namespace.
 *
 * @deprecated Configure necessary client policies by overriding
 * {@link org.springframework.data.aerospike.config.AerospikeDataConfigurationSupport#getClientPolicy()}
 *
 * @author Oliver Gierke
 * @author Peter Milne
 */
@Deprecated
public class AerospikeNamespaceHandler extends NamespaceHandlerSupport {

	@Override
	public void init() {

		// TODO: add declarations and namespaces for other top-level configuration elements
		registerBeanDefinitionParser("client", new AerospikeClientBeanDefinitionParser());
		registerBeanDefinitionParser("clientPolicy", new ClientPolicyBeanDefinitionParser());
		registerBeanDefinitionParser("readPolicy", new ReadPolicyBeanDefinitionParser());
		registerBeanDefinitionParser("writePolicy", new WritePolicyBeanDefinitionParser());
		registerBeanDefinitionParser("queryPolicy", new QueryPolicyBeanDefinitionParser());
		registerBeanDefinitionParser("scanPolicy", new ScanPolicyBeanDefinitionParser());
		registerBeanDefinitionParser("batchPolicy", new BatchPolicyBeanDefinitionParser());
	}
}
