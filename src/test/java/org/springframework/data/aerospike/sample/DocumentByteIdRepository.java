package org.springframework.data.aerospike.sample;

import org.springframework.data.aerospike.repository.AerospikeRepository;

public interface DocumentByteIdRepository extends AerospikeRepository<SampleClasses.DocumentWithByteId, Byte> {

}
