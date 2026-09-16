package org.springframework.data.aerospike.examples.blocking.transactions.repository;

import org.springframework.data.aerospike.examples.blocking.transactions.entity.BlockingTransactionalMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface BlockingTransactionalMovieRepository
    extends AerospikeRepository<BlockingTransactionalMovieDocument, String> {
}
