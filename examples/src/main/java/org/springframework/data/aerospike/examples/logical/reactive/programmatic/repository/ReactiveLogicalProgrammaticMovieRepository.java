package org.springframework.data.aerospike.examples.logical.reactive.programmatic.repository;

import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

public interface ReactiveLogicalProgrammaticMovieRepository
    extends ReactiveAerospikeRepository<LogicalMovieDocument, String> {
}
