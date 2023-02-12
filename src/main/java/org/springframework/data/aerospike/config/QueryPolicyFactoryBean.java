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

import com.aerospike.client.policy.QueryPolicy;
import org.springframework.beans.factory.FactoryBean;

/**
 * A {@link FactoryBean} implementation that exposes the setters necessary to configure a {@link QueryPolicy} via XML.
 *
 * @author Peter Milne
 */
public class QueryPolicyFactoryBean extends ReadPolicyFactoryBean {

    private final QueryPolicy policy;

    /**
     * Creates a new {@link QueryPolicyFactoryBean}.
     */
    public QueryPolicyFactoryBean() {
        this.policy = new QueryPolicy();
    }

    /**
     * Configures the maximum number of concurrent requests to server nodes at any point in time. If there are 16 nodes
     * in the cluster and maxConcurrentNodes is 8, then queries will be made to 8 nodes in parallel.  When a query
     * completes, a new query will be issued until all 16 nodes have been queried. Default (0) is to issue requests to
     * all server nodes in parallel.
     *
     * @param maxConcurrentNodes The maxConcurrentNodes configuration value.
     */
    public void setMaxConcurrentNodes(int maxConcurrentNodes) {
        this.policy.maxConcurrentNodes = maxConcurrentNodes;
    }

    /**
     * Configure the number of records to place in queue before blocking. Records received from multiple server nodes
     * will be placed in a queue. A separate thread consumes these records in parallel. If the queue is full, the
     * producer threads will block until records are consumed.
     *
     * @param recordQueueSize The recordQueueSize configuration value.
     */
    public void setRecordQueueSize(int recordQueueSize) {
        this.policy.recordQueueSize = recordQueueSize;
    }

    @Override
    public QueryPolicy getObject() throws Exception {
        return policy;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public Class<?> getObjectType() {
        return QueryPolicy.class;
    }
}
