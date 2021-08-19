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

import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

/**
 * A {@link FactoryBean} implementation that exposes the setters necessary to configure a read policy via XML.
 * 
 * @author Peter Milne
 */
public class WritePolicyFactoryBean extends ReadPolicyFactoryBean {

	private final WritePolicy policy;

	/**
	 * Creates a new {@link WritePolicyFactoryBean}.
	 */
	public WritePolicyFactoryBean() {
		this.policy = new WritePolicy();
	}

	/**
	 * Configures  consistency guarantee when committing a transaction on the server. The default 
	 * (COMMIT_ALL) indicates that the server should wait for master and all replica commits to 
	 * be successful before returning success to the client.
	 * @param commitLevel The commitLevel configuration value.
	 */
	public void setCommitLevel(CommitLevel commitLevel){
		this.policy.commitLevel = commitLevel;
	}
	
	/**
	 * Configures Record expiration. Also known as ttl (time to live).
	 * Seconds record will live before being removed by the server.
	 * @param expiration The expiration configuration value.
	 */
	public void setExpiration(int expiration){
		this.policy.expiration = expiration;
	}

	/**
	 * Configures the expected generation. Generation is the number of times a record has been modified
	 * (including creation) on the server. If a write operation is creating a record, 
	 * the expected generation would be <code>0</code>.  
	 * @param generation The generation configuration value.
	 */
	public void setGeneration(int generation){
		this.policy.generation = generation;
	}

	/**
	 * 
	 * Configure how to handle record writes based on record generation. The default (NONE)
	 * indicates that the generation is not used to restrict writes.
	 * @param generationPolicy The generationPolicy configuration value.
	 */
	public void setGenerationPolicy(GenerationPolicy generationPolicy){
		this.policy.generationPolicy = generationPolicy;
	}
	
	/**
	 * QConfigure how to handle writes where the record already exists.
	 * @param recordExistsAction The recordExistsAction configuration value.
	 */
	public void setRecordExistsAction(RecordExistsAction recordExistsAction){
		this.policy.recordExistsAction = recordExistsAction;
	}
	
	/**
	 * Configure sending the user defined key in addition to hash digest on a record put.  
	 * The default is to not send the user defined key.
	 * @param sendKey The sendKey configuration value.
	 */
	public void setSendKey(boolean sendKey){
		this.policy.sendKey = sendKey;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObject()
	 */
	@Override
	public WritePolicy getObject() throws Exception {
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
		return WritePolicy.class;
	}
}
