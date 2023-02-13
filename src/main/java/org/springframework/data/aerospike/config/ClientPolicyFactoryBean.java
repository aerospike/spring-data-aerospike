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

import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.springframework.beans.factory.FactoryBean;

/**
 * A {@link FactoryBean} implementation that exposes the setters necessary to configure a {@link ClientPolicy} via XML.
 *
 * @author Peter Milne
 */
public class ClientPolicyFactoryBean implements FactoryBean<ClientPolicy> {

    private final ClientPolicy policy;

    /**
     * Creates a new {@link ClientPolicyFactoryBean}.
     */
    public ClientPolicyFactoryBean() {
        this.policy = new ClientPolicy();
    }

    /**
     * Configures the maximum number of connections for operation processing. This value is used to size the synchronous
     * connection pool for each server node.
     *
     * @param maxConnsPerNode The maxConnsPerNode configuration value.
     */
    public void setMaxConnsPerNode(int maxConnsPerNode) {
        this.policy.maxConnsPerNode = maxConnsPerNode;
    }

    /**
     * Configures the timeout for a client connection when opening a connection to the server host for the first time.
     *
     * @param timeout The timeout configuration value.
     */
    public void setTimeout(int timeout) {
        this.policy.timeout = timeout;
    }

    /**
     * Configures the maximum socket idle time for the client. Socket connection pools will discard sockets that have
     * been idle longer than the maximum
     *
     * @param maxSocketIdle The maxSocketIdle configuration value.
     */
    public void setMaxSocketIdle(int maxSocketIdle) {
        this.policy.maxSocketIdle = maxSocketIdle;
    }

    /**
     * Configures the action if the client cannot connect to a host. If true the client will throw exception if host
     * connection fails during connection.
     *
     * @param failIfNotConnected The failIfNotConnected configuration value.
     */
    public void failIfNotConnected(boolean failIfNotConnected) {
        this.policy.failIfNotConnected = failIfNotConnected;
    }

    /**
     * Configures the tendInterval, in milliseconds, between cluster tends, by maintenance thread.
     *
     * @param tendInterval The tendInterval configuration value.
     */
    public void setTendInterval(int tendInterval) {
        this.policy.tendInterval = tendInterval;
    }

    /**
     * Configures the default read policy
     *
     * @param readPolicy The readPolicy configuration value.
     */
    public void setReadPolicyDefault(Policy readPolicy) {
        this.policy.readPolicyDefault = readPolicy;
    }

    /**
     * Configures the default write policy
     *
     * @param writePolicy The writePolicy configuration value.
     */
    public void setWritePolicyDefault(WritePolicy writePolicy) {
        this.policy.writePolicyDefault = writePolicy;
    }

    /**
     * Configures the default scan policy
     *
     * @param scanPolicy The scanPolicy configuration value.
     */
    public void setScanPolicyDefault(ScanPolicy scanPolicy) {
        this.policy.scanPolicyDefault = scanPolicy;
    }

    /**
     * Configures the default batch policy
     *
     * @param batchPolicy The batchPolicy configuration value.
     */
    public void setBatchPolicyDefault(BatchPolicy batchPolicy) {
        this.policy.batchPolicyDefault = batchPolicy;
    }

    /**
     * Configures the default query policy
     *
     * @param queryPolicy The queryPolicy configuration value.
     */
    public void setQueryPolicyDefault(QueryPolicy queryPolicy) {
        this.policy.queryPolicyDefault = queryPolicy;
    }

    /**
     * Configures the username for authentication to cluster. Only used for clusters running with security enabled.
     *
     * @param user The user configuration value.
     */
    public void setUser(String user) {
        this.policy.user = user;
    }

    /**
     * Configures the User password for authentication to cluster. Only used for clusters running with security
     * enabled.
     *
     * @param password The password configuration value.
     */
    public void setPassword(String password) {
        this.policy.password = password;
    }

    @Override
    public ClientPolicy getObject() throws Exception {
        return policy;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public Class<?> getObjectType() {
        return ClientPolicy.class;
    }
}
