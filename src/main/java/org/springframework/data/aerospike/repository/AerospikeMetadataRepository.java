package org.springframework.data.aerospike.repository;

import org.springframework.data.aerospike.query.FilterOperation;
import org.springframework.data.aerospike.repository.query.CriteriaDefinition.AerospikeMetadata;

import java.util.List;

public interface AerospikeMetadataRepository<T> {

    /**
     * Find entities that have the given metadata field using a unary operation.
     *
     * @param metadataFieldName Metadata field name.
     * @param operation         Operation to be applied (EQ, NOTEQ, LT, LTEQ, GT or GTEQ).
     * @param value             Numerical value of the metadata field.
     * @return Iterable of entities
     */
    Iterable<T> findByMetadata(AerospikeMetadata metadataFieldName, FilterOperation operation, long value);

    /**
     * Find entities that have the given metadata field using BETWEEN operation.
     *
     * @param metadataFieldName Metadata field name.
     * @param operation         BETWEEN operation.
     * @param value1            Lower bound value.
     * @param value2            Upper bound value.
     * @return Iterable of entities
     */
    Iterable<T> findByMetadata(AerospikeMetadata metadataFieldName, FilterOperation operation, long value1, long value2);


    /**
     * Find entities that have the given metadata field using IN/NOT_IN operation.
     *
     * @param metadataFieldName Metadata field name.
     * @param operation         Operation with multiple parameters to be applied (IN or NOT_IN).
     * @param values            Values to be searched through.
     * @return Iterable of entities
     */
    Iterable<T> findByMetadata(AerospikeMetadata metadataFieldName, FilterOperation operation, List<Long> values);
}
