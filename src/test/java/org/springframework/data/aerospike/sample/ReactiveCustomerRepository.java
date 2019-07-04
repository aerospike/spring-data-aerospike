package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

/**
 * Simple reactive repository interface managing {@link Customer}s.
 *
 * @author Igor Ermolenko
 */
public interface ReactiveCustomerRepository extends ReactiveAerospikeRepository<Customer, String> {

}
