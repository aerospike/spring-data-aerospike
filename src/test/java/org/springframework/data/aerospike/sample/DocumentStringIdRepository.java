package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface DocumentStringIdRepository extends AerospikeRepository<SampleClasses.DocumentWithStringId, String> {

}
