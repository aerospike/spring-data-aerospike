package org.springframework.data.aerospike.repository;

import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.query.Qualifier;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata;

public interface AerospikeMetadataRepository<T> {

    /**
     * Find entities that have the given metadata field using a unary operation.
     *
     * @param metadataFieldName Metadata field name.
     * @param operation         Operation to be applied (EQ, NOTEQ, LT, LTEQ, GT, GTEQ, BETWEEN, IN or NOT_IN).
     * @param values            One or more numerical values (depending on the operation).
     * @return Iterable of entities
     */
    Iterable<T> findByMetadata(AerospikeMetadata metadataFieldName, FilterOperation operation, long... values);

    Iterable<T> findByQualifiers(Qualifier... qualifiers);
}
