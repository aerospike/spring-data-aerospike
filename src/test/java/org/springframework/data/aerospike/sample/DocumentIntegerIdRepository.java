package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface DocumentIntegerIdRepository extends AerospikeRepository<SampleClasses.DocumentWithIntegerId, Integer> {

}
