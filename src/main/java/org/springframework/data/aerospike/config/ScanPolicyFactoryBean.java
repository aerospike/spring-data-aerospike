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

import com.aerospike.client.policy.ScanPolicy;
import org.springframework.beans.factory.FactoryBean;

/**
 * A {@link FactoryBean} implementation that exposes the setters necessary to configure a {@link ScanPolicy} via XML.
 *
 * @author Peter Milne
 */
public class ScanPolicyFactoryBean extends ReadPolicyFactoryBean {

    private final ScanPolicy policy;

    /**
     * Creates a new {@link ScanPolicyFactoryBean}.
     */
    public ScanPolicyFactoryBean() {
        this.policy = new ScanPolicy();
    }

    /**
     * Configures scan requests to be issued in parallel or serially.
     *
     * @param concurrentNodes The concurrentNodes configuration value.
     */
    public void setConcurrentNodes(boolean concurrentNodes) {
        this.policy.concurrentNodes = concurrentNodes;
    }

    /**
     * Indicates if bin data is retrieved. If false, only record digests are retrieved.
     *
     * @param includeBinData The includeBinData configuration value.
     */
    public void setIncludeBinData(boolean includeBinData) {
        this.policy.includeBinData = includeBinData;
    }

    /**
     * Configures the maximum number of concurrent requests to server nodes at any point in time. If there are 16 nodes
     * in the cluster and maxConcurrentNodes is 8, then scan requests will be made to 8 nodes in parallel.  When a scan
     * completes, a new scan request will be issued until all 16 nodes have been scanned.
     * <p>
     * This property is only relevant when concurrentNodes is true. Default (0) is to issue requests to all server nodes
     * in parallel.
     *
     * @param maxConcurrentNodes The maxConcurrentNodes configuration value.
     */
    public void setMaxConcurrentNodes(int maxConcurrentNodes) {
        this.policy.maxConcurrentNodes = maxConcurrentNodes;
    }

    @Override
    public ScanPolicy getObject() throws Exception {
        return policy;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public Class<?> getObjectType() {
        return ScanPolicy.class;
    }
}
