package org.springframework.data.aerospike.examples.blocking.indexed.repository;

import org.springframework.data.aerospike.examples.blocking.indexed.entity.AnnotatedMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

// tag::indexed-annotation-repository[]
public interface AnnotatedMovieRepository extends AerospikeRepository<AnnotatedMovieDocument, String> {

    List<AnnotatedMovieDocument> findByGenre(String genre);
}
// end::indexed-annotation-repository[]
