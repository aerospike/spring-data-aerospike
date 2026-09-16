package org.springframework.data.aerospike.examples.reactive.customquery.repository;

import org.springframework.data.aerospike.examples.reactive.customquery.entity.ReactiveProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

public interface ReactiveProgrammaticCustomQueryMovieRepository
    extends ReactiveAerospikeRepository<ReactiveProgrammaticCustomQueryMovieDocument, String> {
}
