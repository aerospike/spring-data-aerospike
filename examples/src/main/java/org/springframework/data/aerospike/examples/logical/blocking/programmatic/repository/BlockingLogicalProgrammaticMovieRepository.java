package org.springframework.data.aerospike.examples.logical.blocking.programmatic.repository;

import org.springframework.data.aerospike.examples.logical.entity.LogicalMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface BlockingLogicalProgrammaticMovieRepository extends AerospikeRepository<LogicalMovieDocument, String> {
}
