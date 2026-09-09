package org.springframework.data.aerospike.examples.reactive.crud.repository;

import org.springframework.data.aerospike.examples.reactive.crud.entity.ReactiveMovieDocument;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

public interface ReactiveMovieRepository extends ReactiveAerospikeRepository<ReactiveMovieDocument, String> {
}
