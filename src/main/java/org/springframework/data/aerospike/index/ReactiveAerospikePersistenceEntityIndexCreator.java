/*
 * Copyright 2020 the original author or authors.
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


package org.springframework.data.aerospike.index;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.aerospike.IndexAlreadyExistsException;
import org.springframework.data.aerospike.core.ReactiveAerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Set;

/**
 * @author Taras Danylchuk
 */
@Slf4j
public class ReactiveAerospikePersistenceEntityIndexCreator extends BaseAerospikePersistenceEntityIndexCreator {

    private final ReactiveAerospikeTemplate template;

    public ReactiveAerospikePersistenceEntityIndexCreator(AerospikeMappingContext aerospikeMappingContext,
                                                          ReactiveAerospikeTemplate template) {
        super(aerospikeMappingContext);
        this.template = template;
    }

    @Override
    protected void installIndexes(Set<AerospikeIndexDefinition> indexes) {
        Flux.fromIterable(indexes)
                .flatMap(this::installIndex)
                .then()
                //blocking for having context fail fast in case any issues with index creation
                .block();
    }

    private Mono<Void> installIndex(AerospikeIndexDefinition index) {
        log.debug("Installing aerospike index: {}...", index);
        return template.createIndex(index.getEntityClass(), index.getName(), index.getFieldName(), index.getType(), index.getCollectionType())
                .doOnSuccess(__ -> log.info("Installed aerospike index: {} successfully.", index))
                .onErrorResume(IndexAlreadyExistsException.class, e -> onIndexAlreadyExists(e, index))
                .doOnError(throwable -> log.error("Failed to install aerospike index: " + index, throwable));
    }

    private Mono<? extends Void> onIndexAlreadyExists(Throwable throwable, AerospikeIndexDefinition indexDefinition) {
        log.info("Skipping index [{}] creation. Index with the same name already exists. {}", indexDefinition, throwable.getMessage());
        return Mono.empty();
    }
}
