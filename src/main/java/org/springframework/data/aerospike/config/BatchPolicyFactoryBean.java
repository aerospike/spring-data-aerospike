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

import org.springframework.beans.factory.FactoryBean;

import com.aerospike.client.policy.BatchPolicy;

/**
 * A {@link FactoryBean} implementation that exposes the setters necessary to configure a {@link BatchPolicy} via XML.
 *
 * @author Peter Milne
 */
public class BatchPolicyFactoryBean extends ReadPolicyFactoryBean {

	private final BatchPolicy policy;

	/**
	 * Creates a new {@link BatchPolicyFactoryBean}.
	 */
	public BatchPolicyFactoryBean() {
		this.policy = new BatchPolicy();
	}

	/**
	 * Configures the maximum number of concurrent batch request threads to server nodes at any point in time
	 *
	 * @param maxConcurrentThreads The maxConcurrentThreads configuration value.
	 */
	public void setMaxConcurrentThreads(int maxConcurrentThreads){
		this.policy.maxConcurrentThreads = maxConcurrentThreads;
	}

	@Override
	public BatchPolicy getObject() throws Exception {
		return policy;
	}

	@Override
	public boolean isSingleton() {
		return false;
	}

	@Override
	public Class<?> getObjectType() {
		return BatchPolicy.class;
	}
}
