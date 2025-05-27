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
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikeMappingContext;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.server.version.ServerVersionSupport;
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
    protected final String namespace;
    protected final AerospikeExceptionTranslator exceptionTranslator;
    protected final WritePolicy writePolicyDefault;
    protected final BatchWritePolicy batchWritePolicyDefault;
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

    public <T> String getSetName(Class<T> entityClass) {
        return mappingContext.getRequiredPersistentEntity(entityClass).getSetName();
    }

    public <T> String getSetName(T document) {
        return mappingContext.getRequiredPersistentEntity(document.getClass()).getSetName();
    }

    public MappingContext<?, ?> getMappingContext() {
        return this.mappingContext;
    }

    public MappingAerospikeConverter getAerospikeConverter() {
        return this.converter;
    }

    public ServerVersionSupport getServerVersionSupport() {
        return this.serverVersionSupport;
    }

    public String getNamespace() {
        return namespace;
    }
}
