package org.springframework.data.aerospike.examples.combined.reactive.programmatic.repository;

import org.springframework.data.aerospike.examples.combined.entity.Movie;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

public interface ReactiveCustomQueryRepository
    extends ReactiveAerospikeRepository<Movie, String> {
}
