package org.springframework.data.aerospike.examples.blocking.query.repository;

import org.springframework.data.aerospike.examples.blocking.query.entity.IndexedMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface IndexedMovieRepository extends AerospikeRepository<IndexedMovieDocument, String> {

    List<IndexedMovieDocument> findByGenre(String genre);
}
