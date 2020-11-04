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
import org.springframework.data.aerospike.core.AerospikeTemplate;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;

import java.util.Set;

/**
 * @author Taras Danylchuk
 */
@Slf4j
public class AerospikePersistenceEntityIndexCreator extends BaseAerospikePersistenceEntityIndexCreator {

    private final AerospikeTemplate template;

    public AerospikePersistenceEntityIndexCreator(AerospikeMappingContext aerospikeMappingContext,
                                                  AerospikeTemplate template) {
        super(aerospikeMappingContext);
        this.template = template;
    }

    @Override
    protected void installIndexes(Set<AerospikeIndexDefinition> indexes) {
        indexes.forEach(this::installIndex);
    }

    private void installIndex(AerospikeIndexDefinition indexDefinition) {
        log.debug("Installing aerospike index: {}...", indexDefinition);
        try {
            template.createIndex(indexDefinition.getEntityClass(), indexDefinition.getName(),
                    indexDefinition.getFieldName(), indexDefinition.getType());
            log.info("Installed aerospike index: {} successfully.", indexDefinition);
        } catch (IndexAlreadyExistsException e) {
            log.info("Skipping index [{}] creation. Index with the same name already exists. {}", indexDefinition, e.getMessage());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to install aerospike index: " + indexDefinition, e);
        }
    }

}
