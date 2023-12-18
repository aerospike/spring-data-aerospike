/*
 * Copyright 2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	  https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.aerospike.query.cache;

import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.query.model.IndexesInfo;
import org.springframework.data.aerospike.utility.ServerVersionUtils;
import reactor.core.publisher.Mono;

/**
 * @author Sergii Karpenko
 */
public class ReactorIndexRefresher {

    private static final Logger log = LoggerFactory.getLogger(ReactorIndexRefresher.class);

    private final IAerospikeReactorClient client;
    private final InfoPolicy infoPolicy;
    private final ServerVersionUtils serverVersionUtils;
    private final InternalIndexOperations indexOperations;
    private final IndexesCacheUpdater indexesCacheUpdater;

    public ReactorIndexRefresher(IAerospikeReactorClient client, InfoPolicy infoPolicy,
                                 InternalIndexOperations indexOperations, IndexesCacheUpdater indexesCacheUpdater,
                                 ServerVersionUtils serverVersionUtils) {
        this.client = client;
        this.infoPolicy = infoPolicy;
        this.indexOperations = indexOperations;
        this.indexesCacheUpdater = indexesCacheUpdater;
        this.serverVersionUtils = serverVersionUtils;
    }

    public Mono<Void> refreshIndexes() {
        return client.info(infoPolicy, null, indexOperations.buildGetIndexesCommand())
            .doOnSubscribe(subscription -> log.trace("Loading indexes"))
            .doOnNext(indexInfo -> {
                IndexesInfo cache = indexOperations.parseIndexesInfo(indexInfo);
                indexOperations.enrichIndexesWithCardinality(client.getAerospikeClient(), cache.indexes,
                    serverVersionUtils);
                this.indexesCacheUpdater.update(cache);
                log.debug("Loaded indexes: {}", cache.indexes);
            }).then();
    }

    public Mono<Void> clearCache() {
        return Mono.fromRunnable(() -> indexesCacheUpdater.update(IndexesInfo.empty()));
    }
}
