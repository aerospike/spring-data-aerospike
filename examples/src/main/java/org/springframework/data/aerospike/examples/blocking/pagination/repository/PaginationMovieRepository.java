package org.springframework.data.aerospike.examples.blocking.pagination.repository;

import org.springframework.data.aerospike.examples.blocking.pagination.entity.PaginationMovieDocument;
import org.springframework.data.aerospike.repository.AerospikeRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;

import java.util.List;

// tag::pagination-repository[]
public interface PaginationMovieRepository extends AerospikeRepository<PaginationMovieDocument, String> {

    List<PaginationMovieDocument> findByGenre(String genre, Sort sort);

    Page<PaginationMovieDocument> findByReleaseYearLessThan(int releaseYear, Pageable pageable);

    Slice<PaginationMovieDocument> findByReleaseYearGreaterThan(int releaseYear, Pageable pageable);

    Page<PaginationMovieDocument> findAllById(Iterable<String> ids, Pageable pageable);
}
// end::pagination-repository[]
