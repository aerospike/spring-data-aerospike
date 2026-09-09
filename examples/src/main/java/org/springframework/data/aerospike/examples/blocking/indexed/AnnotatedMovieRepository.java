package org.springframework.data.aerospike.examples.blocking.indexed;

import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface AnnotatedMovieRepository extends AerospikeRepository<AnnotatedMovieDocument, String> {

    List<AnnotatedMovieDocument> findByGenre(String genre);
}
