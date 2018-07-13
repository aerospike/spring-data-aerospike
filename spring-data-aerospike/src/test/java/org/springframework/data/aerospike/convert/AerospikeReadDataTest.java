/**
 *
 */
package org.springframework.data.aerospike.convert;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

public class AerospikeReadDataTest {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void shouldThrowExceptionIfRecordIsNull() throws Exception {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Record must not be null");

		AerospikeReadData.forRead(new Key("namespace", "set", 867), null);
	}

	@Test
	public void shouldThrowExceptionIfRecordBinsIsNull() throws Exception {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Record bins must not be null");

		AerospikeReadData.forRead(new Key("namespace", "set", 867), new Record(null, 0, 0));
	}

	@Test
	public void shouldThrowExceptionIfKeyIsNull() throws Exception {
		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("Key must not be null");

		AerospikeReadData.forRead(null, new Record(Collections.emptyMap(), 0, 0));
	}
}
