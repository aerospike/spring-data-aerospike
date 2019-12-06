package org.springframework.data.aerospike.repository;

import org.springframework.data.repository.Repository;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;

/**
 * Aerospike specific {@link Repository} interface with reactive support.
 *
 * @author Igor Ermolenko
 */
public interface ReactiveAerospikeRepository<T, ID> extends ReactiveCrudRepository<T, ID> {

}
