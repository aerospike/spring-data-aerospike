package org.springframework.data.aerospike.examples.blocking.projection;

import org.springframework.data.aerospike.repository.AerospikeRepository;

import java.util.List;

public interface ProjectionMovieRepository extends AerospikeRepository<ProjectedMovieDocument, String> {

    List<MovieSummary> findMovieSummaryById(String id);

    <T> List<T> findById(String id, Class<T> type);
}
