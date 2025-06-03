package org.springframework.data.aerospike.core;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.reactor.IAerospikeReactorClient;
import lombok.Builder;
import org.springframework.data.aerospike.convert.MappingAerospikeConverter;
import org.springframework.data.aerospike.mapping.AerospikePersistentProperty;
import org.springframework.data.aerospike.mapping.BasicAerospikePersistentEntity;
import org.springframework.data.aerospike.query.QueryEngine;
import org.springframework.data.aerospike.query.ReactorQueryEngine;
import org.springframework.data.mapping.context.MappingContext;

/**
 * This class aggregates essential components and configurations provided via {@link AerospikeTemplate} and
 * {@link ReactiveAerospikeTemplate}, centralizing the dependencies for database operations.
 */
@Builder
public class TemplateContext {

    IAerospikeClient client;
    IAerospikeReactorClient reactorClient;
    String namespace;
    MappingAerospikeConverter converter;
    MappingContext<BasicAerospikePersistentEntity<?>, AerospikePersistentProperty> mappingContext;
    AerospikeExceptionTranslator exceptionTranslator;
    WritePolicy writePolicyDefault;
    BatchWritePolicy batchWritePolicyDefault;
    QueryEngine queryEngine;
    ReactorQueryEngine reactorQueryEngine;
}
