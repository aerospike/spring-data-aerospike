/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.aerospike.core;

import com.aerospike.client.Log;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.WritePolicy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
import org.springframework.data.mapping.MappingException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

/**
 * Base class for Aerospike templates
 *
 * @author Anastasiia Smirnova
 * @author Igor Ermolenko
 */
@Slf4j
abstract class BaseAerospikeTemplate {

    protected final MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;
    protected final MappingAerospikeConverter converter;
    @Getter
    protected final String namespace;
    protected final AerospikeExceptionTranslator exceptionTranslator;
    protected final WritePolicy writePolicyDefault;
    protected final BatchWritePolicy batchWritePolicyDefault;
    @Getter
    protected final ServerVersionSupport serverVersionSupport;

    BaseAerospikeTemplate(String namespace,
                          MappingAerospikeConverter converter,
                          AerospikeMappingContext mappingContext,
                          AerospikeExceptionTranslator exceptionTranslator,
                          WritePolicy writePolicyDefault,
                          ServerVersionSupport serverVersionSupport) {
        Assert.notNull(writePolicyDefault, "Write policy must not be null!");
        Assert.notNull(namespace, "Namespace cannot be null");
        Assert.hasLength(namespace, "Namespace cannot be empty");

        this.converter = converter;
        this.exceptionTranslator = exceptionTranslator;
        this.namespace = namespace;
        this.mappingContext = mappingContext;
        this.writePolicyDefault = writePolicyDefault;
        this.batchWritePolicyDefault = getFromWritePolicy(writePolicyDefault);
        this.serverVersionSupport = serverVersionSupport;

        loggerSetup();
    }

    /**
     * Defines the types of operations that can be performed on an Aerospike record.
     */
    protected enum OperationType {
        SAVE_OPERATION("save"),
        INSERT_OPERATION("insert"),
        UPDATE_OPERATION("update"),
        DELETE_OPERATION("delete");

        private final String name;

        OperationType(String name) {
            this.name = name;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Constructs a {@link BatchWritePolicy} using a {@link WritePolicy}.
     * This method copies relevant fields from the {@code WritePolicy} to a new {@code BatchWritePolicy} instance.
     *
     * @param writePolicy The {@link WritePolicy} to copy fields from
     * @return A new {@link BatchWritePolicy} populated with values from the given {@code WritePolicy}
     */
    private BatchWritePolicy getFromWritePolicy(WritePolicy writePolicy) {
        BatchWritePolicy batchWritePolicy = new BatchWritePolicy();
        batchWritePolicy.commitLevel = writePolicy.commitLevel;
        batchWritePolicy.durableDelete = writePolicy.durableDelete;
        batchWritePolicy.generationPolicy = writePolicy.generationPolicy;
        batchWritePolicy.expiration = writePolicy.expiration;
        batchWritePolicy.sendKey = writePolicy.sendKey;
        batchWritePolicy.recordExistsAction = writePolicy.recordExistsAction;
        batchWritePolicy.filterExp = writePolicy.filterExp;
        return batchWritePolicy;
    }

    /**
     * Sets up the logger for the Aerospike client.
     * This configures a callback to route Aerospike client logs to SLF4J using a logger named "com.aerospike.client".
     */
    private void loggerSetup() {
        Logger log = LoggerFactory.getLogger("com.aerospike.client");
        Log.setCallback((level, message) -> {
            switch (level) {
                case INFO -> log.info("{}", message);
                case DEBUG -> log.debug("{}", message);
                case ERROR -> log.error("{}", message);
                case WARN -> log.warn("{}", message);
            }
        });
    }

    /**
     * Retrieves the set name for a given entity class.
     * This method queries the mapping context to get the required persistent entity and then its set name.
     *
     * @param <T> The type of the entity
     * @param entityClass The class of the entity
     * @return The set name associated with the entity class
     * @throws MappingException if the entity class is not found in the mapping context
     */
    public <T> String getSetName(Class<T> entityClass) {
        return mappingContext.getRequiredPersistentEntity(entityClass).getSetName();
    }

    /**
     * Retrieves the set name for a given document object.
     * This method determines the class of the document and then queries the mapping context for its set name.
     *
     * @param <T> The type of the document
     * @param document The document object
     * @return The set name associated with the document's class
     * @throws MappingException if the document's class is not found in the mapping context
     */
    public <T> String getSetName(T document) {
        return mappingContext.getRequiredPersistentEntity(document.getClass()).getSetName();
    }

    /**
     * Returns the {@link MappingContext} used by this template.
     * The mapping context provides metadata about how domain objects are mapped to Aerospike records.
     *
     * @return The {@link MappingContext}
     */
    public MappingContext<?, ?> getMappingContext() {
        return this.mappingContext;
    }

    /**
     * Returns the {@link MappingAerospikeConverter} used by this template.
     * The converter is responsible for converting between domain objects and Aerospike records.
     *
     * @return The {@link MappingAerospikeConverter}
     */
    public MappingAerospikeConverter getAerospikeConverter() {
        return this.converter;
    }
}
