/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.aerospike.query.cache;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.query.model.IndexesInfo;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author Anastasiia Smirnova
 */
public class IndexRefresher {

    public static final String INDEX_CACHE_REFRESH_SECONDS = "index.cache.refresh.seconds";
    private final Logger log = LoggerFactory.getLogger(IndexRefresher.class);

    private final IAerospikeClient client;
    private final InfoPolicy infoPolicy;
    private final ServerVersionSupport serverVersionSupport;
    private final InternalIndexOperations indexOperations;
    private final IndexesCacheUpdater indexesCacheUpdater;
    private final ScheduledExecutorService executorService;

    public IndexRefresher(IAerospikeClient client, InfoPolicy infoPolicy, InternalIndexOperations indexOperations,
                          IndexesCacheUpdater indexesCacheUpdater, ServerVersionSupport serverVersionSupport) {
        this.client = client;
        this.infoPolicy = infoPolicy;
        this.indexOperations = indexOperations;
        this.indexesCacheUpdater = indexesCacheUpdater;
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.serverVersionSupport = serverVersionSupport;
    }

    public void scheduleRefreshIndexes(long intervalSeconds) {
        executorService.scheduleWithFixedDelay(this::refreshIndexes, intervalSeconds, intervalSeconds,
            TimeUnit.SECONDS);
    }

    public void refreshIndexes() {
        log.trace("Loading indexes");
        IndexesInfo cache = Arrays.stream(client.getNodes())
            .filter(Node::isActive)
            .findAny() // we do want to send info request to the random node (sending request to the first node may
            // lead to uneven request distribution)
            .map(node -> Info.request(infoPolicy, node, indexOperations.buildGetIndexesCommand()))
            .map(response -> {
                IndexesInfo indexesInfo = indexOperations.parseIndexesInfo(response);
                indexOperations.enrichIndexesWithCardinality(client, indexesInfo.indexes, serverVersionSupport);
                return indexesInfo;
            })
            .orElse(IndexesInfo.empty());
        log.debug("Loaded indexes: {}", cache.indexes);
        this.indexesCacheUpdater.update(cache);
    }

    public void clearCache() {
        indexesCacheUpdater.update(IndexesInfo.empty());
    }
}
