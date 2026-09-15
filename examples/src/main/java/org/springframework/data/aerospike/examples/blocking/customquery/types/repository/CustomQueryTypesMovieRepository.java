package org.springframework.data.aerospike.examples.blocking.customquery.types.repository;

import org.springframework.data.aerospike.examples.blocking.customquery.types.entity.CustomQueryTypesMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

// tag::custom-query-types-repository[]
public interface CustomQueryTypesMovieRepository extends AerospikeRepository<CustomQueryTypesMovieDocument, String> {
}
// end::custom-query-types-repository[]
