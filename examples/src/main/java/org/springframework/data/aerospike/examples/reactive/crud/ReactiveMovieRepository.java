package org.springframework.data.aerospike.examples.reactive.crud;

import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

public interface ReactiveMovieRepository extends ReactiveAerospikeRepository<ReactiveMovieDocument, String> {
}
