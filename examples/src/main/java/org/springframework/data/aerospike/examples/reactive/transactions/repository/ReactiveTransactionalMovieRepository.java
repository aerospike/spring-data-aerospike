package org.springframework.data.aerospike.examples.reactive.transactions.repository;

import org.springframework.data.aerospike.examples.reactive.transactions.entity.ReactiveTransactionalMovieDocument;
import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

public interface ReactiveTransactionalMovieRepository
    extends ReactiveAerospikeRepository<ReactiveTransactionalMovieDocument, String> {
}
