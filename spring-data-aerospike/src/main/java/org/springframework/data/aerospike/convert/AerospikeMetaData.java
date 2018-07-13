package org.springframework.data.aerospike.convert;

/**
 * Carries metadata keys of an aerospike read or written object.
 * @author Anastasiia Smirnova
 */
public interface AerospikeMetaData {

	//sometimes aerospike does not retrieve userKey
	//so we need to save it as a Bin
	//see https://github.com/aerospike/aerospike-client-java/issues/77
	String USER_KEY = "@user_key";

}
