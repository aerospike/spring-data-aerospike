package org.springframework.data.aerospike.examples.blocking.customquery.programmatic.repository;

import org.springframework.data.aerospike.examples.blocking.customquery.programmatic.entity.ProgrammaticCustomQueryMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface ProgrammaticCustomQueryMovieRepository
    extends AerospikeRepository<ProgrammaticCustomQueryMovieDocument, String> {
}
