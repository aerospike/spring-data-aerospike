/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.config;

import org.springframework.beans.factory.FactoryBean;

import com.aerospike.client.policy.ConsistencyLevel;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.Priority;

/**
 * A {@link FactoryBean} implementation that exposes the setters necessary to configure a {@link ReadPolicy} via XML.
 * 
 * @author Peter Milne
 */
public class ReadPolicyFactoryBean implements FactoryBean<Policy> {

	private final Policy policy;

	/**
	 * Creates a new {@link ReadPolicyFactoryBean}.
	 */
	public ReadPolicyFactoryBean() {
		this.policy = new Policy();
	}

	/**
	 * Configures the timeout for each transaction attempt of an operation.
	 * @param socketTimeout
	 */
	public void setSocketTimeout(int socketTimeout){
		this.policy.socketTimeout = socketTimeout;
	}

	/**
	 * Configures the timeout for an operation.
	 * @param totalTimeout
	 */
	public void setTotalTimeout(int totalTimeout){
		this.policy.totalTimeout = totalTimeout;
	}

	/**
	 * Configures the maximum number of retries before aborting the current transaction.
	 * A retry is attempted when there is a network error other than timeout.  
	 * If maxRetries is exceeded, the abort will occur even if the timeout 
	 * has not yet been exceeded.  The default number of retries is 1.
	 */
	public void setMaxRetries(int maxRetries){
		this.policy.maxRetries = maxRetries;
	}

	/**
	 * Configures the sleep between retries if a transaction fails and the 
	 * timeout was not exceeded.  Enter zero to skip sleep.
	 * The default sleep between retries is 500 ms.
	 */
	public void setSleepBetweenRetries(int sleepBetweenRetries){
		this.policy.sleepBetweenRetries = sleepBetweenRetries;
	}

	/**
	 * Configures how replicas should be consulted in a read operation to provide the desired
	 * consistency guarantee. Default to allowing one replica to be used in the
	 * read operation.
	 */
	public void setConsistencyLevel(ConsistencyLevel consistencyLevel){
		this.policy.consistencyLevel = consistencyLevel;
	}

	/**
	 * Configures the priority of request relative to other transactions.
	 * Currently, only used for scans.
	 */
	public void setPriority(Priority priority){
		this.policy.priority = priority;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public Policy getObject() throws Exception {
		return policy;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	@Override
	public boolean isSingleton() {
		return false;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	@Override
	public Class<?> getObjectType() {
		return Policy.class;
	}
}
