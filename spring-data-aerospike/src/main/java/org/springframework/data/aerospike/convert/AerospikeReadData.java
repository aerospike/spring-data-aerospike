package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.springframework.util.Assert;

import java.util.Map;

/**
 * Value object to carry data to be read in object conversion.
 * @author Oliver Gierke
 * @author Anastasiia Smirnova
 */
public class AerospikeReadData {

	private final Key key;
	private final Map<String, Object> record;
	private final int expiration;

	private AerospikeReadData(Key key, Map<String, Object> record, int expiration) {
		this.key = key;
		this.record = record;
		this.expiration = expiration;
	}

	public static AerospikeReadData forRead(Key key, Record record) {
		Assert.notNull(key, "Key must not be null");
		Assert.notNull(record, "Record must not be null");
		Assert.notNull(record.bins, "Record bins must not be null");

		return new AerospikeReadData(key, record.bins, record.getTimeToLive());
	}

	public Map<String, Object> getRecord() {
		return record;
	}

	public Key getKey() {
		return key;
	}

	public Object getValue(String key) {
		return record.get(key);
	}

	public int getExpiration() {
		return expiration;
	}
}
