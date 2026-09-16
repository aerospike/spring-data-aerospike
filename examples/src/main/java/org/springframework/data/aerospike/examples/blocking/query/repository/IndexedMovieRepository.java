package org.springframework.data.aerospike.examples.blocking.query.repository;

import org.springframework.data.aerospike.examples.blocking.query.entity.IndexedMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

// tag::secondary-index-repository[]
public interface IndexedMovieRepository extends AerospikeRepository<IndexedMovieDocument, String> {

    // tag::secondary-index-exact-query-method[]
    List<IndexedMovieDocument> findByGenre(String genre);
    // end::secondary-index-exact-query-method[]

    // tag::secondary-index-unsupported-containing-method[]
    List<IndexedMovieDocument> findByGenreContaining(String genre);
    // end::secondary-index-unsupported-containing-method[]

    // tag::secondary-index-missing-index-method[]
    List<IndexedMovieDocument> findByTitle(String title);
    // end::secondary-index-missing-index-method[]
}
// end::secondary-index-repository[]
