package org.springframework.data.aerospike.examples.blocking.crud;

import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface MovieRepository extends AerospikeRepository<MovieDocument, String> {
}
