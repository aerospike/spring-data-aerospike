package org.springframework.data.aerospike.examples.blocking.crud.repository;

import org.springframework.data.aerospike.examples.blocking.crud.entity.MovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

// tag::blocking-crud-repository[]
public interface MovieRepository extends AerospikeRepository<MovieDocument, String> {
}
// end::blocking-crud-repository[]
