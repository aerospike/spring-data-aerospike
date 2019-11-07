package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.ReactiveAerospikeRepository;

public interface ReactiveCompositeObjectRepository extends ReactiveAerospikeRepository<CompositeObject, String> {

}