package org.springframework.data.aerospike.examples.blocking.query;

import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface IndexedMovieRepository extends AerospikeRepository<IndexedMovieDocument, String> {

    List<IndexedMovieDocument> findByGenre(String genre);
}
