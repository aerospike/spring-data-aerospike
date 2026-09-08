package org.springframework.data.aerospike.examples.blocking.customquery;

import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface CustomQueryMovieRepository extends AerospikeRepository<CustomQueryMovieDocument, String> {
}
