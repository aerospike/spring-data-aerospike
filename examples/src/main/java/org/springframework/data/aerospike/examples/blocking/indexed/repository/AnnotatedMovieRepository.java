package org.springframework.data.aerospike.examples.blocking.indexed.repository;

import org.springframework.data.aerospike.examples.blocking.indexed.entity.AnnotatedMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface AnnotatedMovieRepository extends AerospikeRepository<AnnotatedMovieDocument, String> {

    List<AnnotatedMovieDocument> findByGenre(String genre);
}
