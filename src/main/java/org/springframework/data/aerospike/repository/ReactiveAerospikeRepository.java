package org.springframework.data.aerospike.repository;

import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

/**
 * Reactive aerospike specific {@link Repository}
 *
 * @author Igor Ermolenko
 */
@NoRepositoryBean
public interface ReactiveAerospikeRepository<T, ID> extends ReactiveCrudRepository<T, ID> {

}
