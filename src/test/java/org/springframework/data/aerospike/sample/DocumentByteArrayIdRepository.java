package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface DocumentByteArrayIdRepository extends AerospikeRepository<SampleClasses.DocumentWithByteArrayId, byte[]> {

}
