package org.springframework.data.aerospike.examples.combined.blocking.programmatic.repository;

import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface BlockingCustomQueryRepository extends AerospikeRepository<Movie, String> {
}
