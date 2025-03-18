package org.springframework.data.aerospike.index;

import reactor.core.publisher.Mono;

public interface ReactiveIndexesCacheRefresher extends BaseIndexesCacheRefresher {

    Mono<Void> refreshIndexesCache();
}
